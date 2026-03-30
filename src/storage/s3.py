"""S3 storage backend — AWS S3 + 8 compatible providers.

Supported providers:
- AWS S3, MinIO, Wasabi, OVH, Scaleway,
  DigitalOcean Spaces, Cloudflare R2, Backblaze S3
"""

import logging
from pathlib import Path
from typing import BinaryIO

from src.storage.base import StorageBackend, long_path_mkdir, long_path_str, with_retry

logger = logging.getLogger(__name__)

PROVIDER_ENDPOINTS = {
    "aws": None,  # Default AWS endpoint
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
    "aws": [
        "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1", "eu-central-2",
        "eu-north-1", "eu-south-1", "eu-south-2",
        "us-east-1", "us-east-2", "us-west-1", "us-west-2",
        "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ap-northeast-2",
        "ap-south-1", "ca-central-1", "sa-east-1", "me-south-1", "af-south-1",
    ],
    "wasabi": [
        "eu-central-1", "eu-central-2", "eu-west-1", "eu-west-2",
        "us-east-1", "us-east-2", "us-central-1", "us-west-1",
        "ap-northeast-1", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2",
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
        provider: str = "aws",
    ):
        super().__init__()
        self._bucket = bucket
        self._prefix = prefix.strip("/")
        self._region = region
        self._access_key = access_key
        self._secret_key = secret_key
        self._endpoint_url = endpoint_url or self._resolve_endpoint(provider, region)
        self._provider = provider

    def _resolve_endpoint(self, provider: str, region: str) -> str:
        """Resolve endpoint URL for a provider."""
        template = PROVIDER_ENDPOINTS.get(provider)
        if template is None:
            return ""
        return template.format(region=region, account_id="")

    def _get_client(self):
        """Create a boto3 S3 client."""
        import boto3

        kwargs = {
            "service_name": "s3",
            "region_name": self._region,
            "aws_access_key_id": self._access_key,
            "aws_secret_access_key": self._secret_key,
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

        if self._bandwidth_limit_kbps > 0 or self._progress_callback:
            with open(local_path, "rb") as f:
                reader = self._get_throttled_reader(f)
                client.upload_fileobj(
                    reader,
                    self._bucket,
                    key,
                    Callback=self._make_progress_cb(file_size),
                )
        else:
            client.upload_file(str(local_path), self._bucket, key)

    @with_retry(max_retries=3, base_delay=2.0)
    def upload_file(self, fileobj: BinaryIO, remote_path: str, size: int = 0) -> None:
        """Stream a file-like object to S3."""
        client = self._get_client()
        key = self._s3_key(remote_path)
        reader = self._get_throttled_reader(fileobj)
        client.upload_fileobj(
            reader,
            self._bucket,
            key,
            Callback=self._make_progress_cb(size),
        )

    def list_backups(self) -> list[dict]:
        """List top-level backups in the S3 prefix."""
        client = self._get_client()
        prefix = f"{self._prefix}/" if self._prefix else ""
        logger.info(
            "list_backups: bucket=%s prefix=%r endpoint=%s",
            self._bucket, prefix, self._endpoint_url,
        )
        backups = []

        # Use delimiter to get top-level "folders" and files
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self._bucket, Prefix=prefix, Delimiter="/")

        for page in pages:
            # Common prefixes (directories) — no LastModified from S3 API.
            for cp in page.get("CommonPrefixes", []):
                dir_prefix = cp["Prefix"]
                name = dir_prefix.rstrip("/").rsplit("/", 1)[-1]
                # Fetch the newest object inside this prefix to get a date.
                mtime = self._get_prefix_mtime(client, dir_prefix)
                backups.append(
                    {
                        "name": name,
                        "size": 0,
                        "modified": mtime,
                        "is_dir": True,
                    }
                )

            # Objects (files)
            for obj in page.get("Contents", []):
                key = obj["Key"]
                name = key.rsplit("/", 1)[-1]
                if name and key != prefix:
                    last_mod = obj.get("LastModified", 0)
                    if hasattr(last_mod, "timestamp"):
                        last_mod = last_mod.timestamp()
                    backups.append(
                        {
                            "name": name,
                            "size": obj.get("Size", 0),
                            "modified": last_mod,
                            "is_dir": False,
                        }
                    )

        return backups

    def _get_prefix_mtime(self, client, dir_prefix: str) -> float:
        """Get the modification time of the newest object under a prefix.

        S3 CommonPrefixes (virtual directories) have no LastModified.
        This method fetches one object from the prefix to approximate
        the backup date.

        Args:
            client: boto3 S3 client.
            dir_prefix: The S3 prefix ending with '/'.

        Returns:
            Unix timestamp of the newest object, or 0 if empty.
        """
        try:
            resp = client.list_objects_v2(
                Bucket=self._bucket,
                Prefix=dir_prefix,
                MaxKeys=1,
            )
            for obj in resp.get("Contents", []):
                last_mod = obj.get("LastModified", 0)
                if hasattr(last_mod, "timestamp"):
                    return last_mod.timestamp()
                return float(last_mod) if last_mod else 0.0
        except Exception:
            logger.warning("Failed to get mtime for prefix %s", dir_prefix)
        return 0.0

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
        for i in range(0, len(objects_to_delete), 1000):
            batch = objects_to_delete[i : i + 1000]
            client.delete_objects(
                Bucket=self._bucket,
                Delete={"Objects": batch},
            )

        logger.info("Deleted %d objects for backup %s", len(objects_to_delete), remote_name)

    def test_connection(self) -> tuple[bool, str]:
        """Test S3 connection."""
        try:
            client = self._get_client()
            client.head_bucket(Bucket=self._bucket)

            # Count objects
            prefix = f"{self._prefix}/" if self._prefix else ""
            client.list_objects_v2(Bucket=self._bucket, Prefix=prefix, MaxKeys=1)

            return True, f"Connected to {self._bucket} ({self._provider})"
        except Exception as e:
            return False, f"S3 Error: {type(e).__name__}: {e}"

    def get_free_space(self) -> int | None:
        """S3 has unlimited space."""
        return None

    def get_file_size(self, remote_name: str) -> int | None:
        """Get size of an S3 object."""
        try:
            client = self._get_client()
            key = self._s3_key(remote_name)
            response = client.head_object(Bucket=self._bucket, Key=key)
            return response.get("ContentLength")
        except Exception:
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
        """Download a backup from S3 to a local directory."""
        long_path_mkdir(local_dir)
        dst = local_dir / remote_name
        long_path_mkdir(dst)

        client = self._get_client()
        prefix = self._s3_key(remote_name)
        if not prefix.endswith("/"):
            prefix += "/"

        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                rel = key[len(prefix) :]
                if not rel:
                    continue
                local_file = dst / rel
                long_path_mkdir(local_file.parent)
                client.download_file(self._bucket, key, long_path_str(local_file))
        return dst

    def _make_progress_cb(self, total: int):
        """Create a progress callback for boto3."""
        sent = [0]

        def callback(bytes_amount):
            sent[0] += bytes_amount
            if self._progress_callback and total > 0:
                self._progress_callback(sent[0], total)

        return callback if self._progress_callback else None
