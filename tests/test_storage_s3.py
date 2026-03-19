"""Tests for S3 storage backend using moto mock."""

import pytest

try:
    import boto3
    from moto import mock_aws
    HAS_MOTO = True
except ImportError:
    HAS_MOTO = False
    # Provide a no-op decorator so class definitions don't fail at import
    def mock_aws(func=None):
        if func is not None:
            return func
        return lambda f: f

from src.storage.s3 import S3Storage, PROVIDER_ENDPOINTS

pytestmark = pytest.mark.skipif(not HAS_MOTO, reason="moto not installed")

TEST_BUCKET = "test-backup-bucket"
TEST_REGION = "us-east-1"
TEST_PREFIX = "backups"


@pytest.fixture
def s3_backend():
    """Create an S3Storage backend backed by moto mock."""
    with mock_aws():
        client = boto3.client("s3", region_name=TEST_REGION)
        client.create_bucket(Bucket=TEST_BUCKET)
        backend = S3Storage(
            bucket=TEST_BUCKET,
            prefix=TEST_PREFIX,
            region=TEST_REGION,
            access_key="testing",
            secret_key="testing",
        )
        yield backend


@pytest.fixture
def sample_file(tmp_path):
    """Create a sample file for upload tests."""
    f = tmp_path / "test.txt"
    f.write_text("Hello S3 backup!", encoding="utf-8")
    return f


@pytest.fixture
def sample_dir(tmp_path):
    """Create a sample directory for upload tests."""
    d = tmp_path / "backup_dir"
    d.mkdir()
    (d / "file1.txt").write_text("Content one", encoding="utf-8")
    sub = d / "subdir"
    sub.mkdir()
    (sub / "file2.txt").write_text("Content two", encoding="utf-8")
    return d


class TestS3StorageConnection:
    """Test connection and endpoint resolution."""

    @mock_aws
    def test_test_connection_success(self):
        """Test successful connection check."""
        client = boto3.client("s3", region_name=TEST_REGION)
        client.create_bucket(Bucket=TEST_BUCKET)
        backend = S3Storage(
            bucket=TEST_BUCKET,
            region=TEST_REGION,
            access_key="testing",
            secret_key="testing",
        )
        ok, msg = backend.test_connection()
        assert ok is True
        assert TEST_BUCKET in msg

    @mock_aws
    def test_test_connection_missing_bucket(self):
        """Test connection to non-existent bucket."""
        backend = S3Storage(
            bucket="nonexistent-bucket",
            region=TEST_REGION,
            access_key="testing",
            secret_key="testing",
        )
        ok, msg = backend.test_connection()
        assert ok is False
        assert "S3 Error" in msg

    def test_resolve_endpoint_aws(self):
        """AWS uses default endpoint (None)."""
        backend = S3Storage(bucket="b", provider="aws")
        assert backend._endpoint_url == ""

    def test_resolve_endpoint_wasabi(self):
        """Wasabi resolves a templated endpoint."""
        backend = S3Storage(
            bucket="b", provider="wasabi", region="eu-central-1",
        )
        assert "wasabisys.com" in backend._endpoint_url

    def test_resolve_endpoint_custom(self):
        """Custom endpoint_url overrides provider."""
        custom = "https://my.storage.example.com"
        backend = S3Storage(
            bucket="b", provider="aws", endpoint_url=custom,
        )
        assert backend._endpoint_url == custom


class TestS3StorageUpload:
    """Test file upload operations."""

    def test_upload_single_file(self, s3_backend, sample_file):
        """Upload a single file and verify it appears in listing."""
        s3_backend.upload(sample_file, "test_backup.txt")
        backups = s3_backend.list_backups()
        names = [b["name"] for b in backups]
        assert "test_backup.txt" in names

    def test_upload_directory(self, s3_backend, sample_dir):
        """Upload a directory and verify its contents."""
        s3_backend.upload(sample_dir, "my_backup")
        # List using raw client to check nested keys
        client = s3_backend._get_client()
        response = client.list_objects_v2(
            Bucket=TEST_BUCKET, Prefix=f"{TEST_PREFIX}/my_backup/",
        )
        keys = [obj["Key"] for obj in response.get("Contents", [])]
        assert any("file1.txt" in k for k in keys)
        assert any("file2.txt" in k for k in keys)

    def test_upload_file_stream(self, s3_backend):
        """Upload via file-like object (streaming)."""
        import io
        data = b"Streamed content for backup"
        fileobj = io.BytesIO(data)
        s3_backend.upload_file(fileobj, "stream_test.bin", size=len(data))

        size = s3_backend.get_file_size("stream_test.bin")
        assert size == len(data)

    def test_upload_with_progress(self, s3_backend, sample_file):
        """Upload with progress callback."""
        progress_calls = []
        s3_backend.set_progress_callback(
            lambda sent, total: progress_calls.append((sent, total))
        )
        s3_backend.upload(sample_file, "progress_test.txt")
        assert len(progress_calls) > 0


class TestS3StorageList:
    """Test backup listing."""

    def test_list_backups_empty(self, s3_backend):
        """Empty bucket returns empty list."""
        backups = s3_backend.list_backups()
        assert backups == []

    def test_list_backups_with_files(self, s3_backend, sample_file):
        """List shows uploaded files."""
        s3_backend.upload(sample_file, "backup1.txt")
        s3_backend.upload(sample_file, "backup2.txt")
        backups = s3_backend.list_backups()
        names = [b["name"] for b in backups]
        assert "backup1.txt" in names
        assert "backup2.txt" in names


class TestS3StorageDelete:
    """Test backup deletion."""

    def test_delete_file(self, s3_backend, sample_file):
        """Delete a single file backup."""
        s3_backend.upload(sample_file, "to_delete.txt")
        s3_backend.delete_backup("to_delete.txt")
        backups = s3_backend.list_backups()
        names = [b["name"] for b in backups]
        assert "to_delete.txt" not in names

    def test_delete_directory(self, s3_backend, sample_dir):
        """Delete a directory backup (all objects under prefix)."""
        s3_backend.upload(sample_dir, "dir_to_delete")
        s3_backend.delete_backup("dir_to_delete")
        client = s3_backend._get_client()
        response = client.list_objects_v2(
            Bucket=TEST_BUCKET, Prefix=f"{TEST_PREFIX}/dir_to_delete/",
        )
        assert response.get("KeyCount", 0) == 0

    def test_delete_nonexistent(self, s3_backend):
        """Delete non-existent backup raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            s3_backend.delete_backup("does_not_exist")


class TestS3StorageMisc:
    """Test utility methods."""

    def test_get_free_space_is_none(self, s3_backend):
        """S3 reports unlimited space (None)."""
        assert s3_backend.get_free_space() is None

    def test_get_file_size(self, s3_backend, sample_file):
        """Get size of uploaded file."""
        s3_backend.upload(sample_file, "sized.txt")
        size = s3_backend.get_file_size("sized.txt")
        assert size == sample_file.stat().st_size

    def test_get_file_size_missing(self, s3_backend):
        """Get size of non-existent file returns None."""
        assert s3_backend.get_file_size("missing.txt") is None

    def test_s3_key_with_prefix(self, s3_backend):
        """S3 key includes prefix."""
        key = s3_backend._s3_key("test.txt")
        assert key == f"{TEST_PREFIX}/test.txt"

    def test_s3_key_without_prefix(self):
        """S3 key without prefix."""
        backend = S3Storage(bucket="b", prefix="")
        assert backend._s3_key("test.txt") == "test.txt"
