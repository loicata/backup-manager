import logging
from pathlib import Path
from typing import Optional

from src.storage.base import StorageBackend, with_retry

logger = logging.getLogger(__name__)


class AzureStorage(StorageBackend):
    """Storage backend for Azure Blob Storage."""

    def _get_client(self):
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError:
            raise ImportError(
                "azure-storage-blob is not installed. "
                "Run: pip install azure-storage-blob"
            )
        return BlobServiceClient.from_connection_string(
            self.config.azure_connection_string
        )

    def _get_container_client(self):
        service = self._get_client()
        return service.get_container_client(self.config.azure_container)

    @with_retry()
    def upload(self, local_path: Path, remote_name: str) -> bool:
        try:
            container = self._get_container_client()
            prefix = f"{self.config.azure_prefix}/{remote_name}".strip("/")

            if local_path.is_file():
                blob_name = prefix
                with open(local_path, "rb") as data:
                    container.upload_blob(blob_name,
                                          self._get_throttled_reader(data),
                                          overwrite=True)
            else:
                for file_path in local_path.rglob("*"):
                    if file_path.is_file():
                        rel = file_path.relative_to(local_path)
                        blob_name = f"{prefix}/{rel}".replace("\\", "/")
                        with open(file_path, "rb") as data:
                            container.upload_blob(blob_name,
                                                  self._get_throttled_reader(data),
                                                  overwrite=True)

            logger.info(f"Uploaded to Azure: {self.config.azure_container}/{prefix}")
            return True
        except Exception as e:
            logger.error(f"Azure upload failed: {e}")
            return False

    def list_backups(self) -> list[dict]:
        try:
            container = self._get_container_client()
            prefix = self.config.azure_prefix.strip("/") + "/"
            blobs = container.walk_blobs(name_starts_with=prefix, delimiter="/")
            backups = []
            for blob in blobs:
                name = blob.name.rstrip("/").split("/")[-1]
                backups.append({
                    "name": name,
                    "size": getattr(blob, "size", 0) or 0,
                    "modified": 0,
                    "is_dir": True,
                })
            return backups
        except Exception as e:
            logger.error(f"Azure list failed: {e}")
            return []

    @with_retry()
    def delete_backup(self, remote_name: str) -> bool:
        try:
            container = self._get_container_client()
            prefix = f"{self.config.azure_prefix}/{remote_name}".strip("/")
            blobs = container.list_blobs(name_starts_with=prefix)
            for blob in blobs:
                container.delete_blob(blob.name)
            return True
        except Exception as e:
            logger.error(f"Azure delete failed: {e}")
            return False

    @with_retry()
    def test_connection(self) -> tuple[bool, str]:
        try:
            container = self._get_container_client()
            container.get_container_properties()
            return True, f"✅ Azure connected: {self.config.azure_container}"
        except Exception as e:
            return False, f"❌ Azure Error: {e}"

    def get_file_size(self, remote_name: str) -> Optional[int]:
        try:
            container = self._get_container()
            prefix = self.config.azure_prefix.strip("/") + "/" if self.config.azure_prefix else ""
            blob = container.get_blob_client(prefix + remote_name)
            props = blob.get_blob_properties()
            return props.size
        except Exception:
            return None
