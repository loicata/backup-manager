"""S3 storage backend — AWS S3 + 8 compatible providers.

Supported providers:
- AWS S3, MinIO, Wasabi, OVH, Scaleway,
  DigitalOcean Spaces, Cloudflare R2, Backblaze S3
"""

import logging
import threading
from pathlib import Path
from typing import BinaryIO

from src.storage.base import StorageBackend, long_path_mkdir, long_path_str, with_retry

logger = logging.getLogger(__name__)

PROVIDER_ENDPOINTS = {
    "Amazon AWS": None,  # Default AWS endpoint
    "wasabi": "https://s3.{region}.wasabisys.com",
    "ovh": "https://s3.{region}.cloud.ovh.net",
    "scaleway": "https://s3.{region}.scw.cloud",
    "digitalocean": "https://{region}.digitaloceanspaces.com",
    "cloudflare": "https://{account_id}.r2.cloudflarestorage.com",
    "backblaze_s3": "https://s3.{region}.backblazeb2.com",
    "other": None,
}

# Regions per provider — first entry is the default for new configurations
PROVIDER_REGIONS: dict[str, list[str]] = {
    "Amazon AWS": [
        "eu-west-1",
        "eu-west-2",
        "eu-west-3",
        "eu-central-1",
        "eu-central-2",
        "eu-north-1",
        "eu-south-1",
        "eu-south-2",
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        "ap-southeast-1",
        "ap-southeast-2",
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-south-1",
        "ca-central-1",
        "sa-east-1",
        "me-south-1",
        "af-south-1",
    ],
    "wasabi": [
        "eu-central-1",
        "eu-central-2",
        "eu-west-1",
        "eu-west-2",
        "us-east-1",
        "us-east-2",
        "us-central-1",
        "us-west-1",
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-southeast-1",
        "ap-southeast-2",
    ],
    "ovh": ["gra", "sbg", "bhs", "de", "uk", "waw"],
    "scaleway": ["fr-par", "nl-ams", "pl-waw"],
    "digitalocean": ["nyc3", "sfo3", "ams3", "sgp1", "fra1", "blr1", "syd1"],
    "cloudflare": ["auto"],
    "backblaze_s3": ["us-west-002", "us-west-004", "eu-central-003"],
    "other": [],
}


class S3Storage(StorageBackend):
    """S3-compatible storage backend."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        region: str = "eu-west-1",
        access_key: str = "",
        secret_key: str = "",
        endpoint_url: str = "",
        provider: str = "Amazon AWS",
    ):
        super().__init__()
        self._bucket = bucket
        self._prefix = prefix.strip("/")
        self._region = region
        self._access_key = access_key
        self._secret_key = secret_key
        self._endpoint_url = endpoint_url or self._resolve_endpoint(provider, region)
        self._provider = provider
        self._retain_until = None  # Object Lock retain-until-date (datetime)

    def set_retain_until(self, retain_until) -> None:
        """Set the Object Lock retain-until-date for subsequent uploads.

        Args:
            retain_until: datetime (UTC) when the lock expires, or None
                to disable per-object retention.
        """
        self._retain_until = retain_until

    def _build_lock_extra_args(self) -> dict:
        """Build ExtraArgs dict for Object Lock uploads.

        Returns:
            Dict with ObjectLockMode and ObjectLockRetainUntilDate if
            retain_until is set, empty dict otherwise.
        """
        if self._retain_until is None:
            return {}
        return {
            "ObjectLockMode": "COMPLIANCE",
            "ObjectLockRetainUntilDate": self._retain_until,
        }

    def _resolve_endpoint(self, provider: str, region: str) -> str:
        """Resolve endpoint URL for a provider."""
        template = PROVIDER_ENDPOINTS.get(provider)
        if template is None:
            return ""
        return template.format(region=region, account_id="")

    def _get_client(self):
        """Create a boto3 S3 client with robust timeout settings."""
        import boto3
        from botocore.config import Config

        config = Config(
            connect_timeout=60,
            read_timeout=600,
            retries={"max_attempts": 5, "mode": "adaptive"},
            s3={"multipart_chunksize": 16 * 1024 * 1024},  # 16 MB parts
        )

        kwargs = {
            "service_name": "s3",
            "region_name": self._region,
            "aws_access_key_id": self._access_key,
            "aws_secret_access_key": self._secret_key,
            "config": config,
        }
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url

        return boto3.client(**kwargs)

    def _s3_key(self, name: str) -> str:
        """Build full S3 key from prefix and name."""
        if self._prefix:
            return f"{self._prefix}/{name}"
        return name

    @with_retry(max_retries=3, base_delay=2.0)
    def upload(self, local_path: Path, remote_name: str) -> None:
        """Upload file or directory to S3."""
        client = self._get_client()

        if local_path.is_dir():
            for filepath in local_path.rglob("*"):
                if filepath.is_file():
                    rel = filepath.relative_to(local_path).as_posix()
                    key = self._s3_key(f"{remote_name}/{rel}")
                    self._upload_one(client, filepath, key)
        else:
            key = self._s3_key(remote_name)
            self._upload_one(client, local_path, key)

    def _upload_one(self, client, local_path: Path, key: str) -> None:
        """Upload a single file to S3 with optional throttling."""
        file_size = local_path.stat().st_size
        extra_args = self._build_lock_extra_args()

        if self._bandwidth_limit_kbps > 0 or self._progress_callback:
            with open(local_path, "rb") as f:
                reader = self._get_throttled_reader(f)
                kwargs: dict = {
                    "Fileobj": reader,
                    "Bucket": self._bucket,
                    "Key": key,
                    "Callback": self._make_progress_cb(file_size),
                }
                if extra_args:
                    kwargs["ExtraArgs"] = extra_args
                client.upload_fileobj(**kwargs)
        else:
            kwargs_file: dict = {
                "Filename": str(local_path),
                "Bucket": self._bucket,
                "Key": key,
            }
            if extra_args:
                kwargs_file["ExtraArgs"] = extra_args
            client.upload_file(**kwargs_file)

    @with_retry(max_retries=3, base_delay=2.0)
    def upload_file(self, fileobj: BinaryIO, remote_path: str, size: int = 0) -> None:
        """Stream a file-like object to S3."""
        client = self._get_client()
        key = self._s3_key(remote_path)
        reader = self._get_throttled_reader(fileobj)
        extra_args = self._build_lock_extra_args()
        kwargs: dict = {
            "Fileobj": reader,
            "Bucket": self._bucket,
            "Key": key,
            "Callback": self._make_progress_cb(size),
        }
        if extra_args:
            kwargs["ExtraArgs"] = extra_args
        client.upload_fileobj(**kwargs)

    def list_backups(self) -> list[dict]:
        """List top-level backups in the S3 prefix."""
        client = self._get_client()
        prefix = f"{self._prefix}/" if self._prefix else ""
        logger.info(
            "list_backups: bucket=%s prefix=%r endpoint=%s",
            self._bucket,
            prefix,
            self._endpoint_url,
        )
        backups = []

        # Use delimiter to get top-level "folders" and files
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self._bucket, Prefix=prefix, Delimiter="/")

        for page in pages:
            # Common prefixes (directories) — no Size or LastModified
            # from S3 API.  Sum all objects inside each prefix.
            for cp in page.get("CommonPrefixes", []):
                dir_prefix = cp["Prefix"]
                name = dir_prefix.rstrip("/").rsplit("/", 1)[-1]
                total_size, mtime, has_wbenc = self._get_prefix_stats(client, dir_prefix)
                backups.append(
                    {
                        "name": name,
                        "size": total_size,
                        "modified": mtime,
                        "is_dir": True,
                        "encrypted": has_wbenc,
                    }
                )

            # Objects (files) — skip manifests (.wbverify) and
            # partial uploads (.partial) as they are not usable backups.
            for obj in page.get("Contents", []):
                key = obj["Key"]
                name = key.rsplit("/", 1)[-1]
                if (
                    name
                    and key != prefix
                    and not name.endswith(".wbverify")
                    and not name.endswith(".partial")
                ):
                    last_mod = obj.get("LastModified", 0)
                    if hasattr(last_mod, "timestamp"):
                        last_mod = last_mod.timestamp()
                    backups.append(
                        {
                            "name": name,
                            "size": obj.get("Size", 0),
                            "modified": last_mod,
                            "is_dir": False,
                            "encrypted": ".wbenc" in name.lower(),
                        }
                    )

        return backups

    def _get_prefix_stats(self, client, dir_prefix: str) -> tuple[int, float, bool]:
        """Get total size, newest modification time, and encryption flag.

        S3 CommonPrefixes (virtual directories) have no Size or
        LastModified.  This method lists all objects under the prefix
        to compute the total size, find the newest modification time,
        and detect encrypted archives (.wbenc files).

        Args:
            client: boto3 S3 client.
            dir_prefix: The S3 prefix ending with '/'.

        Returns:
            Tuple of (total_size_bytes, newest_mtime_timestamp, has_wbenc).
        """
        total_size = 0
        newest_mtime = 0.0
        has_wbenc = False
        try:
            paginator = client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self._bucket, Prefix=dir_prefix)
            for page in pages:
                for obj in page.get("Contents", []):
                    total_size += obj.get("Size", 0)
                    key = obj.get("Key", "")
                    if ".wbenc" in key:
                        has_wbenc = True
                    last_mod = obj.get("LastModified", 0)
                    if hasattr(last_mod, "timestamp"):
                        ts = last_mod.timestamp()
                    else:
                        ts = float(last_mod) if last_mod else 0.0
                    if ts > newest_mtime:
                        newest_mtime = ts
        except Exception:
            logger.warning("Failed to get stats for prefix %s", dir_prefix)
        return total_size, newest_mtime, has_wbenc

    @with_retry(max_retries=3, base_delay=2.0)
    def delete_backup(self, remote_name: str) -> None:
        """Delete a backup (file or prefix) from S3."""
        client = self._get_client()
        prefix = self._s3_key(remote_name)

        # List all objects under this prefix
        objects_to_delete = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                objects_to_delete.append({"Key": obj["Key"]})

        if not objects_to_delete:
            raise FileNotFoundError(f"Backup not found: {remote_name}")

        # Delete in batches of 1000
        failed_deletes: list[dict] = []
        for i in range(0, len(objects_to_delete), 1000):
            batch = objects_to_delete[i : i + 1000]
            response = client.delete_objects(
                Bucket=self._bucket,
                Delete={"Objects": batch},
            )
            # S3 returns HTTP 200 even when individual objects fail to
            # delete (Object Lock retention, legal hold, permissions).
            # Without inspecting ``Errors`` the caller wrongly assumes
            # the backup is gone while it's still billed and still
            # counts as "has_full" in rotation logic.
            batch_errors = response.get("Errors", []) or []
            if batch_errors:
                failed_deletes.extend(batch_errors)

        deleted_count = len(objects_to_delete) - len(failed_deletes)
        if failed_deletes:
            first = failed_deletes[0]
            code = first.get("Code", "Unknown")
            msg = first.get("Message", "")
            sample_key = first.get("Key", "")
            raise OSError(
                f"Failed to delete {len(failed_deletes)}/{len(objects_to_delete)} "
                f"objects for backup {remote_name!r}: first failure — "
                f"{code} on {sample_key!r}: {msg}"
            )

        logger.info("Deleted %d objects for backup %s", deleted_count, remote_name)

    def test_connection(self) -> tuple[bool, str]:
        """Test S3 connection, read access AND write permission.

        A read-only credential passes head_bucket + list_objects but
        fails at the first real upload — after potentially 30 minutes
        of streaming. Probing write at connection time surfaces the
        problem immediately.

        Under Object Lock COMPLIANCE mode the probe object cannot be
        deleted until retention expires, so the probe is skipped there
        to avoid leaving a locked object behind on every click of
        "Test connection". Write will be indirectly proven by the
        first real backup.
        """
        try:
            client = self._get_client()
            client.head_bucket(Bucket=self._bucket)

            # Count objects (confirms list permission)
            prefix = f"{self._prefix}/" if self._prefix else ""
            client.list_objects_v2(Bucket=self._bucket, Prefix=prefix, MaxKeys=1)

            # Probe write permission when safe to do so (no Object Lock)
            if self._retain_until is None:
                self._probe_write(client, prefix)

            return True, f"Connected to {self._bucket} ({self._provider})"
        except Exception as e:
            return False, f"S3 Error: {type(e).__name__}: {e}"

    def _probe_write(self, client, prefix: str) -> None:
        """Upload and delete a small probe object to confirm write access.

        Raises:
            Exception: If the bucket rejects the probe (permissions,
                quota, bucket policy). Surfaces the same way as any
                S3 error in ``test_connection``.
        """
        import uuid

        probe_key = f"{prefix}.backup_manager_probe_{uuid.uuid4().hex[:16]}"
        try:
            client.put_object(
                Bucket=self._bucket,
                Key=probe_key,
                Body=b"probe",
            )
        finally:
            # Best-effort delete — if put failed we still try to clean
            # up in case it partially succeeded. Delete failures on a
            # non-existent key are harmless.
            try:
                client.delete_object(Bucket=self._bucket, Key=probe_key)
            except Exception as e:
                logger.debug("Probe cleanup failed (non-fatal): %s", e)

    def get_free_space(self) -> int | None:
        """S3 has unlimited space."""
        return None

    def get_file_size(self, remote_name: str) -> int | None:
        """Get size of an S3 object."""
        try:
            client = self._get_client()
            key = self._s3_key(remote_name)
            logger.info("get_file_size: bucket=%s key=%r", self._bucket, key)
            response = client.head_object(Bucket=self._bucket, Key=key)
            size = response.get("ContentLength")
            logger.info("get_file_size: found, size=%s", size)
            return size
        except Exception as e:
            logger.warning("get_file_size: not found: %s: %s", type(e).__name__, e)
            return None

    def list_backup_files(self, backup_name: str) -> list[tuple[str, int]]:
        """List files inside a backup prefix on S3.

        Args:
            backup_name: Name of the backup (S3 prefix).

        Returns:
            List of (relative_path, size_bytes) tuples.
        """
        client = self._get_client()
        prefix = self._s3_key(backup_name)
        if not prefix.endswith("/"):
            prefix += "/"

        files: list[tuple[str, int]] = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                rel = key[len(prefix) :]
                if rel:
                    files.append((rel, obj.get("Size", 0)))
        return files

    def verify_backup_files(self, backup_name: str) -> list[tuple[str, int, str]]:
        """Verify backup files using S3 ETags (MD5 for simple uploads).

        The ETag for non-multipart uploads is the MD5 of the object.
        Multipart ETags contain a dash and are not usable as MD5.

        Args:
            backup_name: Name of the backup (S3 prefix).

        Returns:
            List of (relative_path, size_bytes, md5_hex) tuples.
            md5_hex is "" for multipart uploads (ETag contains "-").
        """
        client = self._get_client()
        prefix = self._s3_key(backup_name)
        if not prefix.endswith("/"):
            prefix += "/"

        files: list[tuple[str, int, str]] = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                rel = key[len(prefix) :]
                if not rel:
                    continue
                size = obj.get("Size", 0)
                etag = obj.get("ETag", "").strip('"')
                # Multipart ETags contain "-" and are not MD5
                md5 = etag if "-" not in etag else ""
                files.append((rel, size, md5))
        return files

    def download_backup(self, remote_name: str, local_dir: Path) -> Path:
        """Download a backup from S3 to a local directory.

        Supports two backup formats:
        - Directory backups: prefix containing multiple objects (flat copy)
        - Single file backups: one object (e.g. .tar.wbenc encrypted archive)

        If the destination already exists it is removed first so that
        a re-download always starts from a clean state.

        Args:
            remote_name: Backup name on the remote.
            local_dir: Local directory to download into.

        Returns:
            Path to the downloaded backup (file or directory).
        """
        import shutil

        long_path_mkdir(local_dir)
        client = self._get_client()

        # Try directory download first (prefix with /)
        prefix = self._s3_key(remote_name)
        dir_prefix = prefix if prefix.endswith("/") else prefix + "/"

        downloaded_count = 0
        dst = local_dir / remote_name

        # Check if this is a directory (prefix with children)
        resp = client.list_objects_v2(Bucket=self._bucket, Prefix=dir_prefix, MaxKeys=1)
        is_directory = len(resp.get("Contents", [])) > 0

        # Build download progress callback if set
        dl_progress_cb = self._make_progress_cb(0)  # placeholder, updated per-call

        if is_directory:
            # Directory backup — download all objects under prefix
            if dst.exists():
                # Fail loudly instead of silently merging old + new
                # files (ignore_errors=True used to mask permission
                # denials and locked files, producing a restore with
                # stale residue that was almost impossible to debug).
                try:
                    shutil.rmtree(dst)
                except OSError as e:
                    raise OSError(
                        f"Cannot clear existing download destination {dst}: "
                        f"{e}. Close any application using files inside it "
                        f"and retry."
                    ) from e
            long_path_mkdir(dst)

            # First pass: compute total size for progress
            total_size = 0
            objects_to_download: list[tuple[str, str, int]] = []
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._bucket, Prefix=dir_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    rel = key[len(dir_prefix) :]
                    if not rel:
                        continue
                    obj_size = obj.get("Size", 0)
                    total_size += obj_size
                    objects_to_download.append((key, rel, obj_size))

            # Second pass: download with progress
            dl_progress_cb = self._make_progress_cb(total_size)
            for key, rel, _obj_size in objects_to_download:
                local_file = dst / rel
                long_path_mkdir(local_file.parent)
                dl_kwargs: dict = {
                    "Bucket": self._bucket,
                    "Key": key,
                    "Filename": long_path_str(local_file),
                }
                if dl_progress_cb:
                    dl_kwargs["Callback"] = dl_progress_cb
                client.download_file(**dl_kwargs)
                downloaded_count += 1

            logger.info("Downloaded directory backup %s: %d files", remote_name, downloaded_count)
        else:
            # Single file backup (e.g. encrypted .tar.wbenc)
            key = self._s3_key(remote_name)
            local_file = local_dir / remote_name
            if local_file.exists():
                local_file.unlink()

            # Get file size for progress
            try:
                head = client.head_object(Bucket=self._bucket, Key=key)
                file_size = head.get("ContentLength", 0)
            except Exception:
                file_size = 0
            dl_progress_cb = self._make_progress_cb(file_size)

            dl_kwargs = {
                "Bucket": self._bucket,
                "Key": key,
                "Filename": long_path_str(local_file),
            }
            if dl_progress_cb:
                dl_kwargs["Callback"] = dl_progress_cb
            client.download_file(**dl_kwargs)
            dst = local_file
            logger.info("Downloaded single file backup %s", remote_name)

        # Download .wbverify manifest if present
        manifest_key = self._s3_key(f"{remote_name}.wbverify")
        manifest_local = local_dir / f"{remote_name}.wbverify"
        try:
            client.download_file(self._bucket, manifest_key, long_path_str(manifest_local))
            logger.info("Downloaded manifest: %s.wbverify", remote_name)
        except Exception:
            logger.debug("No manifest found for %s (older backup)", remote_name)

        return dst

    def _make_progress_cb(self, total: int):
        """Create a thread-safe progress callback for boto3.

        boto3's s3transfer worker pool invokes this callback from
        multiple threads during a multipart upload. A plain
        ``sent[0] += bytes_amount`` is not atomic under interleaving,
        which caused occasional backwards jumps in the UI progress
        bar. A lock serializes increments and the snapshot read so the
        reported position is monotonically non-decreasing.
        """
        sent = [0]
        lock = threading.Lock()

        def callback(bytes_amount):
            with lock:
                sent[0] += bytes_amount
                snapshot = sent[0]
            if self._progress_callback and total > 0:
                self._progress_callback(snapshot, total)

        return callback if self._progress_callback else None
