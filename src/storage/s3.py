"""S3 storage backend — AWS S3 + 8 compatible providers.

Supported providers:
- AWS S3, MinIO, Wasabi, OVH, Scaleway,
  DigitalOcean Spaces, Cloudflare R2, Backblaze S3
"""

import logging
from pathlib import Path
from typing import BinaryIO, Optional

from src.storage.base import StorageBackend, with_retry

logger = logging.getLogger(__name__)

PROVIDER_ENDPOINTS = {
    "aws": None,  # Default AWS endpoint
    "minio": "https://localhost:9000",
    "wasabi": "https://s3.{region}.wasabisys.com",
    "ovh": "https://s3.{region}.cloud.ovh.net",
    "scaleway": "https://s3.{region}.scw.cloud",
    "digitalocean": "https://{region}.digitaloceanspaces.com",
    "cloudflare": "https://{account_id}.r2.cloudflarestorage.com",
    "backblaze_s3": "https://s3.{region}.backblazeb2.com",
    "other": None,
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
        backups = []

        # Use delimiter to get top-level "folders" and files
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self._bucket, Prefix=prefix, Delimiter="/")

        for page in pages:
            # Common prefixes (directories)
            for cp in page.get("CommonPrefixes", []):
                name = cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
                backups.append(
                    {
                        "name": name,
                        "size": 0,
                        "modified": 0,
                        "is_dir": True,
                    }
                )

            # Objects (files)
            for obj in page.get("Contents", []):
                key = obj["Key"]
                name = key.rsplit("/", 1)[-1]
                if name and key != prefix:
                    backups.append(
                        {
                            "name": name,
                            "size": obj.get("Size", 0),
                            "modified": obj.get("LastModified", 0),
                            "is_dir": False,
                        }
                    )

        return backups

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
            response = client.list_objects_v2(Bucket=self._bucket, Prefix=prefix, MaxKeys=1)
            count = response.get("KeyCount", 0)

            return True, f"Connected to {self._bucket} ({self._provider})"
        except Exception as e:
            return False, f"S3 Error: {type(e).__name__}: {e}"

    def get_free_space(self) -> Optional[int]:
        """S3 has unlimited space."""
        return None

    def get_file_size(self, remote_name: str) -> Optional[int]:
        """Get size of an S3 object."""
        try:
            client = self._get_client()
            key = self._s3_key(remote_name)
            response = client.head_object(Bucket=self._bucket, Key=key)
            return response.get("ContentLength")
        except Exception:
            return None

    def download_backup(self, remote_name: str, local_dir: Path) -> Path:
        """Download a backup from S3 to a local directory."""
        local_dir.mkdir(parents=True, exist_ok=True)
        dst = local_dir / remote_name
        dst.mkdir(parents=True, exist_ok=True)

        client = self._get_client()
        prefix = self._s3_key(remote_name)
        if not prefix.endswith("/"):
            prefix += "/"

        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                rel = key[len(prefix):]
                if not rel:
                    continue
                local_file = dst / rel
                local_file.parent.mkdir(parents=True, exist_ok=True)
                client.download_file(self._bucket, key, str(local_file))
        return dst

    def _make_progress_cb(self, total: int):
        """Create a progress callback for boto3."""
        sent = [0]

        def callback(bytes_amount):
            sent[0] += bytes_amount
            if self._progress_callback and total > 0:
                self._progress_callback(sent[0], total)

        return callback if self._progress_callback else None
