import logging
from pathlib import Path
from typing import Optional

from src.storage.base import StorageBackend, with_retry

logger = logging.getLogger(__name__)


class GCSStorage(StorageBackend):
    """Storage backend for Google Cloud Storage."""

    def _get_client(self):
        try:
            from google.cloud import storage as gcs_storage
        except ImportError:
            raise ImportError(
                "google-cloud-storage is not installed. "
                "Run: pip install google-cloud-storage"
            )

        if self.config.gcs_credentials_path:
            return gcs_storage.Client.from_service_account_json(
                self.config.gcs_credentials_path
            )
        return gcs_storage.Client()

    def _get_bucket(self):
        client = self._get_client()
        return client.bucket(self.config.gcs_bucket)

    @with_retry()
    def upload(self, local_path: Path, remote_name: str) -> bool:
        try:
            bucket = self._get_bucket()
            prefix = f"{self.config.gcs_prefix}/{remote_name}".strip("/")

            if local_path.is_file():
                blob = bucket.blob(prefix)
                if self._bandwidth_limit_kbps > 0:
                    with open(local_path, "rb") as f:
                        blob.upload_from_file(self._get_throttled_reader(f))
                else:
                    blob.upload_from_filename(str(local_path))
            else:
                for file_path in local_path.rglob("*"):
                    if file_path.is_file():
                        rel = file_path.relative_to(local_path)
                        key = f"{prefix}/{rel}".replace("\\", "/")
                        blob = bucket.blob(key)
                        if self._bandwidth_limit_kbps > 0:
                            with open(file_path, "rb") as f:
                                blob.upload_from_file(self._get_throttled_reader(f))
                        else:
                            blob.upload_from_filename(str(file_path))

            logger.info(f"Uploaded to GCS: gs://{self.config.gcs_bucket}/{prefix}")
            return True
        except Exception as e:
            logger.error(f"GCS upload failed: {e}")
            return False

    def list_backups(self) -> list[dict]:
        try:
            client = self._get_client()
            prefix = self.config.gcs_prefix.strip("/") + "/"
            blobs = client.list_blobs(
                self.config.gcs_bucket, prefix=prefix, delimiter="/"
            )
            # Consume the iterator to get prefixes
            _ = list(blobs)
            backups = []
            for p in blobs.prefixes:
                name = p.rstrip("/").split("/")[-1]
                backups.append({
                    "name": name, "size": 0, "modified": 0, "is_dir": True,
                })
            return backups
        except Exception as e:
            logger.error(f"GCS list failed: {e}")
            return []

    @with_retry()
    def delete_backup(self, remote_name: str) -> bool:
        try:
            client = self._get_client()
            prefix = f"{self.config.gcs_prefix}/{remote_name}".strip("/")
            blobs = client.list_blobs(self.config.gcs_bucket, prefix=prefix)
            for blob in blobs:
                blob.delete()
            return True
        except Exception as e:
            logger.error(f"GCS delete failed: {e}")
            return False

    @with_retry()
    def test_connection(self) -> tuple[bool, str]:
        try:
            bucket = self._get_bucket()
            bucket.reload()
            return True, f"✅ GCS connected: {self.config.gcs_bucket}"
        except Exception as e:
            return False, f"❌ GCS Error: {e}"

    def get_file_size(self, remote_name: str) -> Optional[int]:
        try:
            bucket = self._get_bucket()
            prefix = self.config.gcs_prefix.strip("/") + "/" if self.config.gcs_prefix else ""
            blob = bucket.get_blob(prefix + remote_name)
            if blob:
                return blob.size
        except Exception:
            pass
        return None
