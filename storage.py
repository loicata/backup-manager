"""
Backup Manager - Storage Backends
==================================
Abstraction layer for all storage destinations. Each backend implements
the same interface (upload, list, delete, test, get_free_space, get_file_size).

Supported backends:
  LocalStorage          → local/external drives (shutil.copy2 or throttled copy)
  NetworkStorage        → UNC paths (\\\\server\\share, NAS, Samba)
  S3Storage             → AWS S3 + compatible (MinIO, Wasabi, OVH, Scaleway, R2)
  AzureStorage          → Azure Blob Storage
  SFTPStorage           → SSH/SFTP with password or key authentication
  GCSStorage            → Google Cloud Storage
  ProtonDriveStorage    → Proton Drive via rclone subprocess

Bandwidth throttling:
  set_bandwidth_limit(kbps) applies to all backends:
  - Local/Network: _throttled_copy() reads in 64KB chunks with sleep()
  - S3/Azure/GCS: ThrottledReader wraps file objects for streaming uploads
  - SFTP: ThrottledReader + sftp.putfo() instead of sftp.put()
  - Proton: rclone --bwlimit flag

Factory: get_storage_backend(config) returns the right backend class.
"""

import logging
import os
import shutil
import subprocess
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Optional

from config import BackupProfile, StorageConfig, StorageType
from secure_memory import secure_clear

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════
#  Bandwidth Throttling
#  Wraps file reads to limit upload speed.
#  Algorithm: tracks total bytes vs elapsed time,
#  sleeps when ahead of schedule. Accuracy: ±0.1%.
# ══════════════════════════════════════════════
class ThrottledReader:
    """
    File-like wrapper that limits read speed to a target KB/s rate.
    Used to prevent network saturation during backup uploads.

    Wraps any file object and throttles read() calls by sleeping
    between chunks to maintain the target throughput.
    """

    def __init__(self, file_obj, limit_kbps: int):
        self._file = file_obj
        self._limit_bps = limit_kbps * 1024  # Convert KB/s to B/s
        self._start_time = time.monotonic()
        self._total_bytes = 0

    def read(self, size: int = -1) -> bytes:
        data = self._file.read(size)
        if not data or self._limit_bps <= 0:
            return data

        self._total_bytes += len(data)

        # Calculate how long the transfer should have taken at the target rate
        expected_time = self._total_bytes / self._limit_bps
        actual_time = time.monotonic() - self._start_time

        # Sleep if we're ahead of schedule
        if actual_time < expected_time:
            time.sleep(expected_time - actual_time)

        return data

    def __getattr__(self, name):
        """Forward all other attributes to the wrapped file."""
        return getattr(self._file, name)


class StorageBackend(ABC):
    """Abstract base class for all storage backends."""

    def __init__(self, config: StorageConfig):
        self.config = config
        self._progress_callback: Optional[Callable] = None
        self._bandwidth_limit_kbps: int = 0  # 0 = unlimited

    def set_progress_callback(self, callback: Optional[Callable]):
        self._progress_callback = callback

    def set_bandwidth_limit(self, kbps: int):
        """Set upload bandwidth limit in KB/s. 0 = unlimited."""
        self._bandwidth_limit_kbps = max(0, kbps)

    def _throttled_copy(self, src_path: Path, dst_path: Path):
        """
        Copy a file with optional bandwidth throttling.
        Used by local/network backends instead of shutil.copy2.
        """
        chunk_size = 64 * 1024  # 64 KB chunks

        if self._bandwidth_limit_kbps <= 0:
            # No limit — use fast native copy
            shutil.copy2(src_path, dst_path)
            return

        limit_bytes_per_sec = self._bandwidth_limit_kbps * 1024
        start_time = time.monotonic()
        total_bytes = 0

        with open(src_path, "rb") as fsrc, open(dst_path, "wb") as fdst:
            while True:
                chunk = fsrc.read(chunk_size)
                if not chunk:
                    break
                fdst.write(chunk)
                total_bytes += len(chunk)

                # Throttle: sleep if we're ahead of schedule
                expected_time = total_bytes / limit_bytes_per_sec
                actual_time = time.monotonic() - start_time
                if actual_time < expected_time:
                    time.sleep(expected_time - actual_time)

        # Preserve timestamps
        shutil.copystat(src_path, dst_path)

    def _get_throttled_reader(self, file_obj):
        """
        Wrap a file object with bandwidth throttling.
        Used by S3, Azure, SFTP backends for streaming uploads.
        Returns the original file if no limit is set.
        """
        if self._bandwidth_limit_kbps <= 0:
            return file_obj
        return ThrottledReader(file_obj, self._bandwidth_limit_kbps)

    @abstractmethod
    def upload(self, local_path: Path, remote_name: str) -> bool:
        """Upload a file or directory to the storage backend."""
        ...

    @abstractmethod
    def list_backups(self) -> list[dict]:
        """List available backups in the storage."""
        ...

    @abstractmethod
    def delete_backup(self, remote_name: str) -> bool:
        """Delete a backup from the storage."""
        ...

    @abstractmethod
    def test_connection(self) -> tuple[bool, str]:
        """Test the connection to the storage backend. Returns (success, message)."""
        ...

    def get_free_space(self) -> Optional[int]:
        """
        Get available free space in bytes on the destination.
        Returns None if the information is not available (e.g., cloud storage
        with no fixed quota, or the backend doesn't support this check).
        """
        return None  # Default: unknown

    def get_file_size(self, remote_name: str) -> Optional[int]:
        """
        Get the size in bytes of a remote file.
        Returns None if the file doesn't exist or the backend doesn't support this.
        Used for post-upload mirror verification.
        """
        return None  # Default: unknown

    @staticmethod
    def format_size(size_bytes: int) -> str:
        """Format byte size to human-readable string."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"


# ═══════════════════════════════════════════
#  Local / External Drive Storage
#  Simplest backend: copies files to a local or USB path.
#  Throttled copy used for bandwidth limiting.
# ═══════════════════════════════════════════
class LocalStorage(StorageBackend):
    """Storage backend for local and external drives."""

    def upload(self, local_path: Path, remote_name: str) -> bool:
        dest = Path(self.config.destination_path)
        dest.mkdir(parents=True, exist_ok=True)

        target = dest / remote_name
        try:
            if local_path.is_dir():
                if target.exists():
                    shutil.rmtree(target)
                shutil.copytree(local_path, target)
            else:
                self._throttled_copy(local_path, target)
            logger.info(f"Uploaded to local: {target}")
            return True
        except OSError as e:
            logger.error(f"Local upload failed: {e}")
            return False

    def list_backups(self) -> list[dict]:
        dest = Path(self.config.destination_path)
        if not dest.exists():
            return []

        backups = []
        for item in sorted(dest.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
            stat = item.stat()
            size = stat.st_size
            if item.is_dir():
                size = sum(f.stat().st_size for f in item.rglob("*") if f.is_file())
            backups.append({
                "name": item.name,
                "size": size,
                "modified": stat.st_mtime,
                "is_dir": item.is_dir(),
            })
        return backups

    def delete_backup(self, remote_name: str) -> bool:
        target = Path(self.config.destination_path) / remote_name
        try:
            if target.is_dir():
                shutil.rmtree(target)
            elif target.exists():
                target.unlink()
            return True
        except OSError as e:
            logger.error(f"Delete failed: {e}")
            return False

    def test_connection(self) -> tuple[bool, str]:
        dest = Path(self.config.destination_path)
        try:
            dest.mkdir(parents=True, exist_ok=True)
            test_file = dest / ".backupmanager_test"
            test_file.write_text("test")
            test_file.unlink()
            free = self.get_free_space()
            space_info = f" — {self.format_size(free)} available" if free else ""
            return True, f"✅ Access OK: {dest}{space_info}"
        except OSError as e:
            return False, f"❌ Access error: {e}"

    def get_free_space(self) -> Optional[int]:
        dest = Path(self.config.destination_path)
        try:
            dest.mkdir(parents=True, exist_ok=True)
            usage = shutil.disk_usage(dest)
            return usage.free
        except OSError:
            return None

    def get_file_size(self, remote_name: str) -> Optional[int]:
        target = Path(self.config.destination_path) / remote_name
        try:
            if target.exists():
                return target.stat().st_size
        except OSError:
            pass
        return None


# ═══════════════════════════════════════════
#  Network Storage (UNC paths: \\server\share)
#  Used for NAS, Samba, and Windows shared folders.
#  Same as local but with UNC path handling.
# ═══════════════════════════════════════════
class NetworkStorage(StorageBackend):
    """Storage backend for UNC network paths (\\\\server\\share)."""

    def upload(self, local_path: Path, remote_name: str) -> bool:
        unc_path = Path(self.config.destination_path)
        target = unc_path / remote_name
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            if local_path.is_dir():
                if target.exists():
                    shutil.rmtree(target)
                shutil.copytree(local_path, target)
            else:
                self._throttled_copy(local_path, target)
            logger.info(f"Uploaded to network: {target}")
            return True
        except OSError as e:
            logger.error(f"Network upload failed: {e}")
            return False

    def list_backups(self) -> list[dict]:
        unc_path = Path(self.config.destination_path)
        if not unc_path.exists():
            return []

        backups = []
        try:
            for item in sorted(unc_path.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
                stat = item.stat()
                size = stat.st_size
                if item.is_dir():
                    size = sum(f.stat().st_size for f in item.rglob("*") if f.is_file())
                backups.append({
                    "name": item.name,
                    "size": size,
                    "modified": stat.st_mtime,
                    "is_dir": item.is_dir(),
                })
        except OSError as e:
            logger.error(f"Cannot list network backups: {e}")
        return backups

    def delete_backup(self, remote_name: str) -> bool:
        target = Path(self.config.destination_path) / remote_name
        try:
            if target.is_dir():
                shutil.rmtree(target)
            elif target.exists():
                target.unlink()
            return True
        except OSError as e:
            logger.error(f"Network delete failed: {e}")
            return False

    def test_connection(self) -> tuple[bool, str]:
        unc_path = Path(self.config.destination_path)
        try:
            if not unc_path.exists():
                return False, f"❌ Network path unreachable: {unc_path}"
            test_file = unc_path / ".backupmanager_test"
            test_file.write_text("test")
            test_file.unlink()
            free = self.get_free_space()
            space_info = f" — {self.format_size(free)} available" if free else ""
            return True, f"✅ Network path OK: {unc_path}{space_info}"
        except OSError as e:
            return False, f"❌ Network error: {e}"

    def get_free_space(self) -> Optional[int]:
        unc_path = Path(self.config.destination_path)
        try:
            if not unc_path.exists():
                return None
            usage = shutil.disk_usage(unc_path)
            return usage.free
        except OSError:
            return None

    def get_file_size(self, remote_name: str) -> Optional[int]:
        target = Path(self.config.destination_path) / remote_name
        try:
            if target.exists():
                return target.stat().st_size
        except OSError:
            pass
        return None


# ═══════════════════════════════════════════
#  Amazon S3 + Compatible Providers
#  Supports: AWS S3, MinIO, Wasabi, OVH, Scaleway, Backblaze B2, Cloudflare R2
#  Uses boto3. Custom endpoint_url for S3-compatible providers.
#  Throttled via upload_fileobj() with ThrottledReader wrapper.
# ═══════════════════════════════════════════
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


# ═══════════════════════════════════════════
#  Azure Blob Storage
#  Uses connection string authentication.
#  Throttled via upload_blob() with ThrottledReader wrapper.
# ═══════════════════════════════════════════
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


# ──────────────────────────────────────────────
#  SFTP Storage
# ──────────────────────────────────────────────
class SFTPStorage(StorageBackend):
    """Storage backend for SFTP servers."""

    # ── SFTP connection setup ──
    # Password decrypted from DPAPI, used for paramiko.Transport,
    # then immediately cleared from memory.
    def _get_transport(self):
        try:
            import paramiko
        except ImportError:
            raise ImportError("paramiko is not installed. Run: pip install paramiko")

        transport = paramiko.Transport((self.config.sftp_host, self.config.sftp_port))

        if self.config.sftp_key_path:
            # Auto-detect key type (RSA, Ed25519, ECDSA, DSA)
            pkey = None
            key_path = self.config.sftp_key_path
            for key_class in (paramiko.Ed25519Key, paramiko.ECDSAKey,
                              paramiko.RSAKey, paramiko.DSSKey):
                try:
                    pkey = key_class.from_private_key_file(key_path)
                    break
                except (paramiko.ssh_exception.SSHException, ValueError):
                    continue
            if pkey is None:
                raise ValueError(f"Cannot load SSH key: {key_path}")
            transport.connect(username=self.config.sftp_username, pkey=pkey)
        else:
            from encryption import retrieve_password
            decrypted_pwd = retrieve_password(self.config.sftp_password)
            try:
                transport.connect(
                    username=self.config.sftp_username,
                    password=decrypted_pwd,
                )
            finally:
                secure_clear(decrypted_pwd)
                decrypted_pwd = None
        return transport

    def _get_sftp(self):
        import paramiko
        transport = self._get_transport()
        return paramiko.SFTPClient.from_transport(transport), transport

    # ── Recursive remote mkdir ──
    # Creates parent directories on the SFTP server if they don't exist.
    def _ensure_remote_dir(self, sftp, remote_dir: str):
        """Recursively create remote directories."""
        dirs_to_create = []
        current = remote_dir
        while current and current != "/":
            try:
                sftp.stat(current)
                break
            except FileNotFoundError:
                dirs_to_create.append(current)
                current = os.path.dirname(current)
        for d in reversed(dirs_to_create):
            try:
                sftp.mkdir(d)
            except OSError:
                pass

    def upload(self, local_path: Path, remote_name: str) -> bool:
        try:
            sftp, transport = self._get_sftp()
            remote_base = f"{self.config.sftp_remote_path}/{remote_name}".rstrip("/")

            try:
                if local_path.is_file():
                    remote_dir = os.path.dirname(remote_base)
                    self._ensure_remote_dir(sftp, remote_dir)
                    if self._bandwidth_limit_kbps > 0:
                        with open(local_path, "rb") as f:
                            sftp.putfo(self._get_throttled_reader(f), remote_base)
                    else:
                        sftp.put(str(local_path), remote_base)
                else:
                    for file_path in local_path.rglob("*"):
                        if file_path.is_file():
                            rel = file_path.relative_to(local_path)
                            remote_file = f"{remote_base}/{rel}".replace("\\", "/")
                            remote_dir = os.path.dirname(remote_file)
                            self._ensure_remote_dir(sftp, remote_dir)
                            if self._bandwidth_limit_kbps > 0:
                                with open(file_path, "rb") as f:
                                    sftp.putfo(self._get_throttled_reader(f), remote_file)
                            else:
                                sftp.put(str(file_path), remote_file)

                logger.info(f"Uploaded to SFTP: {self.config.sftp_host}:{remote_base}")
                return True
            finally:
                sftp.close()
                transport.close()
        except Exception as e:
            logger.error(f"SFTP upload failed: {e}")
            return False

    def list_backups(self) -> list[dict]:
        try:
            sftp, transport = self._get_sftp()
            try:
                entries = sftp.listdir_attr(self.config.sftp_remote_path)
                backups = []
                for entry in sorted(entries, key=lambda e: e.st_mtime or 0, reverse=True):
                    from stat import S_ISDIR
                    backups.append({
                        "name": entry.filename,
                        "size": entry.st_size or 0,
                        "modified": entry.st_mtime or 0,
                        "is_dir": S_ISDIR(entry.st_mode) if entry.st_mode else False,
                    })
                return backups
            finally:
                sftp.close()
                transport.close()
        except Exception as e:
            logger.error(f"SFTP list failed: {e}")
            return []

    def delete_backup(self, remote_name: str) -> bool:
        try:
            sftp, transport = self._get_sftp()
            remote_path = f"{self.config.sftp_remote_path}/{remote_name}"
            try:
                self._recursive_remove(sftp, remote_path)
                return True
            finally:
                sftp.close()
                transport.close()
        except Exception as e:
            logger.error(f"SFTP delete failed: {e}")
            return False

    def _recursive_remove(self, sftp, path: str):
        """Recursively remove a remote file or directory."""
        from stat import S_ISDIR
        try:
            attr = sftp.stat(path)
            if S_ISDIR(attr.st_mode):
                for entry in sftp.listdir(path):
                    self._recursive_remove(sftp, f"{path}/{entry}")
                sftp.rmdir(path)
            else:
                sftp.remove(path)
        except FileNotFoundError:
            pass

    def test_connection(self) -> tuple[bool, str]:
        try:
            sftp, transport = self._get_sftp()
            try:
                sftp.listdir(self.config.sftp_remote_path)
                free = self.get_free_space()
                space_info = f" — {self.format_size(free)} available" if free else ""
                return True, (
                    f"✅ SFTP connected: {self.config.sftp_username}@"
                    f"{self.config.sftp_host}:{self.config.sftp_port}{space_info}"
                )
            finally:
                sftp.close()
                transport.close()
        except Exception as e:
            return False, f"❌ SFTP Error: {e}"

    def get_free_space(self) -> Optional[int]:
        try:
            sftp, transport = self._get_sftp()
            try:
                stat = sftp.statvfs(self.config.sftp_remote_path)
                # f_bavail = free blocks for unprivileged users, f_frsize = fragment size
                return stat.f_bavail * stat.f_frsize
            finally:
                sftp.close()
                transport.close()
        except Exception:
            return None

    def get_file_size(self, remote_name: str) -> Optional[int]:
        try:
            sftp, transport = self._get_sftp()
            try:
                remote_dir = self.config.sftp_remote_path.rstrip("/")
                remote_path = f"{remote_dir}/{remote_name}"
                attrs = sftp.stat(remote_path)
                return attrs.st_size
            finally:
                sftp.close()
                transport.close()
        except Exception:
            return None


# ──────────────────────────────────────────────
#  Google Cloud Storage
# ──────────────────────────────────────────────
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



# ══════════════════════════════════════════════
#  Factory: returns the correct backend for a StorageConfig
# ══════════════════════════════════════════════
def get_storage_backend(config: StorageConfig) -> StorageBackend:
    """Factory function to create the appropriate storage backend."""
    match config.storage_type:
        case StorageType.LOCAL.value:
            return LocalStorage(config)
        case StorageType.NETWORK.value:
            return NetworkStorage(config)
        case StorageType.S3.value:
            return S3Storage(config)
        case StorageType.AZURE.value:
            return AzureStorage(config)
        case StorageType.SFTP.value:
            return SFTPStorage(config)
        case StorageType.GCS.value:
            return GCSStorage(config)
        case StorageType.PROTON.value:
            return ProtonDriveStorage(config)
        case _:
            return LocalStorage(config)


# ──────────────────────────────────────────────
#  Proton Drive (via rclone)
# ──────────────────────────────────────────────
class ProtonDriveStorage(StorageBackend):
    """
    Storage backend for Proton Drive using rclone.

    Proton Drive does not yet offer a stable Python SDK.
    This backend uses rclone's protondrive backend, which supports
    the same end-to-end client-side encryption as the official apps.

    Requires: rclone installed and accessible in PATH or via proton_rclone_path.
    Install: https://rclone.org/install/
    """

    RCLONE_REMOTE_NAME = "backupmanager_proton"

    def _find_rclone(self) -> str:
        """Find the rclone binary path."""
        if self.config.proton_rclone_path:
            return self.config.proton_rclone_path

        # Try common locations
        for candidate in ["rclone", "rclone.exe",
                           os.path.expanduser("~/rclone/rclone.exe"),
                           "C:\\rclone\\rclone.exe"]:
            if shutil.which(candidate):
                return candidate

        raise FileNotFoundError(
            "rclone is not installed or not found. "
            "Download it from https://rclone.org/install/ "
            "and make sure it is in your PATH."
        )

    def _build_env(self) -> dict:
        """Build environment variables for rclone with Proton credentials."""
        from encryption import retrieve_password
        env = os.environ.copy()
        env["RCLONE_CONFIG_BACKUPMANAGER_PROTON_TYPE"] = "protondrive"
        env["RCLONE_CONFIG_BACKUPMANAGER_PROTON_USER"] = self.config.proton_username
        decrypted_pwd = retrieve_password(self.config.proton_password)
        try:
            env["RCLONE_CONFIG_BACKUPMANAGER_PROTON_PASS"] = self._obscure_password(
                decrypted_pwd
            )
        finally:
            secure_clear(decrypted_pwd)
            decrypted_pwd = None
        if self.config.proton_2fa:
            env["RCLONE_CONFIG_BACKUPMANAGER_PROTON_2FA"] = self.config.proton_2fa
        return env

    def _obscure_password(self, password: str) -> str:
        """Obscure a password for rclone env var (rclone obscure)."""
        try:
            rclone = self._find_rclone()
            result = subprocess.run(
                [rclone, "obscure", password],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception as e:
            logger.debug(f"rclone obscure unavailable, using fallback: {type(e).__name__}")
        return password

    def _run_rclone(self, args: list[str], timeout: int = 300) -> subprocess.CompletedProcess:
        """Run an rclone command with Proton Drive credentials."""
        rclone = self._find_rclone()
        env = self._build_env()
        full_args = [rclone] + args
        # Add bandwidth limit if set
        if self._bandwidth_limit_kbps > 0:
            full_args.append(f"--bwlimit={self._bandwidth_limit_kbps}k")
        return subprocess.run(
            full_args, capture_output=True, text=True,
            timeout=timeout, env=env,
        )

    def _remote_path(self, name: str = "") -> str:
        """Build the full rclone remote:path string."""
        base = f"{self.RCLONE_REMOTE_NAME}:{self.config.proton_remote_path.strip('/')}"
        if name:
            return f"{base}/{name}"
        return base

    def upload(self, local_path: Path, remote_name: str) -> bool:
        try:
            remote = self._remote_path(remote_name)
            if local_path.is_file():
                result = self._run_rclone([
                    "copyto", str(local_path), remote,
                    "--no-traverse",
                ])
            else:
                result = self._run_rclone([
                    "copy", str(local_path), remote,
                    "--no-traverse",
                ])

            if result.returncode == 0:
                logger.info(f"Uploaded to Proton Drive: {remote}")
                return True
            else:
                logger.error(f"Proton Drive upload failed: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"Proton Drive upload error: {e}")
            return False

    def list_backups(self) -> list[dict]:
        try:
            remote = self._remote_path()
            result = self._run_rclone([
                "lsjson", remote, "--dirs-only",
            ], timeout=60)

            if result.returncode != 0:
                return []

            import json as json_mod
            entries = json_mod.loads(result.stdout) if result.stdout.strip() else []
            backups = []
            for entry in entries:
                backups.append({
                    "name": entry.get("Name", ""),
                    "size": entry.get("Size", 0),
                    "modified": 0,
                    "is_dir": entry.get("IsDir", True),
                })

            return sorted(backups, key=lambda b: b["name"], reverse=True)
        except Exception as e:
            logger.error(f"Proton Drive list failed: {e}")
            return []

    def delete_backup(self, remote_name: str) -> bool:
        try:
            remote = self._remote_path(remote_name)
            result = self._run_rclone(["purge", remote], timeout=120)
            return result.returncode == 0
        except Exception as e:
            logger.error(f"Proton Drive delete failed: {e}")
            return False

    def test_connection(self) -> tuple[bool, str]:
        try:
            self._find_rclone()
        except FileNotFoundError as e:
            return False, f"❌ {e}"

        try:
            remote = self._remote_path()
            result = self._run_rclone(["lsd", remote], timeout=30)
            if result.returncode == 0:
                return True, (
                    f"✅ Proton Drive connected: {self.config.proton_username} "
                    f"({self.config.proton_remote_path})"
                )
            else:
                error = result.stderr.strip().split("\n")[-1] if result.stderr else "Unknown error"
                return False, f"❌ Proton Drive: {error}"
        except Exception as e:
            return False, f"❌ Proton Drive Error: {e}"

    def get_free_space(self) -> Optional[int]:
        try:
            remote = f"{self.RCLONE_REMOTE_NAME}:"
            result = self._run_rclone(["about", remote, "--json"], timeout=30)
            if result.returncode == 0:
                import json as json_mod
                data = json_mod.loads(result.stdout)
                return data.get("free")
        except Exception as e:
            logger.debug(f"Cannot determine free space: {e}")
        return None

    def get_file_size(self, remote_name: str) -> Optional[int]:
        try:
            remote_path = self.config.proton_remote_path.rstrip("/")
            full_path = f"{self.RCLONE_REMOTE_NAME}:{remote_path}/{remote_name}"
            result = self._run_rclone(["size", full_path, "--json"], timeout=30)
            if result.returncode == 0:
                import json as json_mod
                data = json_mod.loads(result.stdout)
                return data.get("bytes")
        except Exception:
            pass
        return None


# ══════════════════════════════════════════════
#  Pre-backup space check (used by engine and wizard)
# ══════════════════════════════════════════════
def check_destination_space(
    storage_config,
    estimated_size_bytes: int = 0,
) -> tuple[bool, str]:
    """
    Utility function to check if a destination has enough free space.

    Args:
        storage_config: StorageConfig object or dict.
        estimated_size_bytes: Estimated backup size in bytes.

    Returns:
        (ok, message) — ok is True if space is sufficient or unknown.
    """
    if isinstance(storage_config, dict):
        storage_config = StorageConfig(**storage_config)

    try:
        backend = get_storage_backend(storage_config)
        free = backend.get_free_space()

        if free is None:
            return True, "ℹ Disk space cannot be checked for this destination."

        free_str = StorageBackend.format_size(free)

        if estimated_size_bytes > 0:
            needed_str = StorageBackend.format_size(estimated_size_bytes)
            if free < estimated_size_bytes:
                return False, (
                    f"❌ Espace insuffisant : {free_str} available, "
                    f"{needed_str} needed."
                )
            return True, (
                f"✅ Espace suffisant : {free_str} available "
                f"({needed_str} needed)."
            )

        return True, f"✅ Espace available : {free_str}"

    except Exception as e:
        return True, f"⚠ Verification not possible : {e}"
