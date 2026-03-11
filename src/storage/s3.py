import logging
from pathlib import Path
from typing import Optional

from src.storage.base import StorageBackend, with_retry

logger = logging.getLogger(__name__)


class S3Storage(StorageBackend):
    """
    Storage backend for Amazon S3 and S3-compatible providers.
    Supports: AWS S3, MinIO, Wasabi, OVH Object Storage, Scaleway,
    DigitalOcean Spaces, Cloudflare R2, Backblaze S3, and any
    provider exposing an S3-compatible API via custom endpoint URL.
    """

    # Pre-configured endpoint templates for common providers
    PROVIDER_ENDPOINTS = {
        "aws":          "",  # Default AWS (no custom endpoint needed)
        "minio":        "http://localhost:9000",
        "wasabi":       "https://s3.{region}.wasabisys.com",
        "ovh":          "https://s3.{region}.cloud.ovh.net",
        "scaleway":     "https://s3.{region}.scw.cloud",
        "digitalocean": "https://{region}.digitaloceanspaces.com",
        "cloudflare":   "https://{account_id}.r2.cloudflarestorage.com",
        "backblaze_s3": "https://s3.{region}.backblazeb2.com",
    }

    def _get_client(self):
        try:
            import boto3
            from botocore.config import Config as BotoConfig
        except ImportError:
            raise ImportError(
                "boto3 is not installed. Run: pip install boto3"
            )

        kwargs = {
            "service_name": "s3",
            "region_name": self.config.s3_region or "us-east-1",
        }

        if self.config.s3_access_key and self.config.s3_secret_key:
            kwargs["aws_access_key_id"] = self.config.s3_access_key
            kwargs["aws_secret_access_key"] = self.config.s3_secret_key

        # Custom endpoint for S3-compatible providers
        endpoint = self._resolve_endpoint()
        if endpoint:
            kwargs["endpoint_url"] = endpoint
            # Most S3-compatible providers need path-style addressing
            kwargs["config"] = BotoConfig(s3={"addressing_style": "path"})

        return boto3.client(**kwargs)

    def _resolve_endpoint(self) -> str:
        """Resolve the endpoint URL from provider preset or custom URL."""
        # Custom URL takes priority
        if self.config.s3_endpoint_url:
            return self.config.s3_endpoint_url.strip()

        # Provider presets
        provider = self.config.s3_provider.lower()
        template = self.PROVIDER_ENDPOINTS.get(provider, "")
        if template:
            return template.format(
                region=self.config.s3_region or "us-east-1",
                account_id=self.config.s3_access_key or "",
            )

        return ""

    @with_retry()
    def upload(self, local_path: Path, remote_name: str) -> bool:
        try:
            s3 = self._get_client()
            prefix = f"{self.config.s3_prefix}/{remote_name}".strip("/")

            if local_path.is_file():
                with open(local_path, "rb") as f:
                    s3.upload_fileobj(
                        self._get_throttled_reader(f),
                        self.config.s3_bucket,
                        prefix,
                    )
            else:
                for file_path in local_path.rglob("*"):
                    if file_path.is_file():
                        rel = file_path.relative_to(local_path)
                        key = f"{prefix}/{rel}".replace("\\", "/")
                        with open(file_path, "rb") as f:
                            s3.upload_fileobj(
                                self._get_throttled_reader(f),
                                self.config.s3_bucket,
                                key,
                            )

            logger.info(f"Uploaded to S3: s3://{self.config.s3_bucket}/{prefix}")
            return True
        except Exception as e:
            logger.error(f"S3 upload failed: {e}")
            return False

    def list_backups(self) -> list[dict]:
        try:
            s3 = self._get_client()
            prefix = self.config.s3_prefix.strip("/") + "/"
            response = s3.list_objects_v2(
                Bucket=self.config.s3_bucket,
                Prefix=prefix,
                Delimiter="/",
            )
            backups = []
            for cp in response.get("CommonPrefixes", []):
                name = cp["Prefix"].rstrip("/").split("/")[-1]
                backups.append({
                    "name": name,
                    "size": 0,
                    "modified": 0,
                    "is_dir": True,
                })
            return backups
        except Exception as e:
            logger.error(f"S3 list failed: {e}")
            return []

    @with_retry()
    def delete_backup(self, remote_name: str) -> bool:
        try:
            s3 = self._get_client()
            prefix = f"{self.config.s3_prefix}/{remote_name}".strip("/")

            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=prefix):
                objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
                if objects:
                    s3.delete_objects(
                        Bucket=self.config.s3_bucket,
                        Delete={"Objects": objects},
                    )
            return True
        except Exception as e:
            logger.error(f"S3 delete failed: {e}")
            return False

    @with_retry()
    def test_connection(self) -> tuple[bool, str]:
        try:
            s3 = self._get_client()
            s3.head_bucket(Bucket=self.config.s3_bucket)
            endpoint = self._resolve_endpoint()
            provider = self.config.s3_provider.upper() or "AWS"
            endpoint_info = f" (endpoint: {endpoint})" if endpoint else ""
            return True, f"✅ S3 connected [{provider}]: {self.config.s3_bucket}{endpoint_info}"
        except Exception as e:
            return False, f"❌ S3 Error: {e}"

    def get_file_size(self, remote_name: str) -> Optional[int]:
        try:
            s3 = self._get_client()
            prefix = self.config.s3_prefix.strip("/") + "/" if self.config.s3_prefix else ""
            key = prefix + remote_name
            obj = s3.head_object(Bucket=self.config.s3_bucket, Key=key)
            return obj.get("ContentLength")
        except Exception:
            return None
