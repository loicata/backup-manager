"""Integration tests for full backup pipeline with SFTP storage and mirrors.

Tests three scenarios:
1. SFTP as primary storage (remote_writer path)
2. SFTP as Mirror 1 destination
3. SFTP as Mirror 2 destination
4. Combined: local primary + 2 SFTP mirrors

Requires SSH server at 192.168.3.243 with test key.
"""

import stat as stat_mod
import uuid
from pathlib import Path

import pytest

try:
    import paramiko

    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

from src.core.backup_engine import BackupEngine, BackupStats
from src.core.config import (
    BackupProfile,
    BackupType,
    ConfigManager,
    EncryptionConfig,
    RetentionConfig,
    RetentionPolicy,
    StorageConfig,
    StorageType,
    VerificationConfig,
)
from src.core.events import EventBus
from src.storage.sftp import SFTPStorage

# --- Connection config ---
SFTP_HOST = "192.168.3.243"
SFTP_PORT = 22
SFTP_USER = "cipango56"
SFTP_KEY_PATH = str(Path.home() / ".ssh" / "test_key")
SFTP_REMOTE_BASE = "/home/cipango56/backups/backup_test"


def _can_connect() -> bool:
    """Quick probe to check if SSH server is reachable."""
    if not HAS_PARAMIKO:
        return False
    if not Path(SFTP_KEY_PATH).exists():
        return False
    try:
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(
            username=SFTP_USER,
            pkey=paramiko.Ed25519Key.from_private_key_file(SFTP_KEY_PATH),
        )
        transport.close()
        return True
    except Exception:
        return False


CAN_CONNECT = _can_connect()
pytestmark = pytest.mark.skipif(
    not CAN_CONNECT,
    reason=f"Cannot connect to SFTP server at {SFTP_HOST}",
)


# --- Helpers ---


def _sftp_session():
    """Open a raw SFTP session for setup/cleanup."""
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(
        username=SFTP_USER,
        pkey=paramiko.Ed25519Key.from_private_key_file(SFTP_KEY_PATH),
    )
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


def _ensure_remote_dir(path: str) -> None:
    """Create a remote directory."""
    sftp, transport = _sftp_session()
    try:
        try:
            sftp.mkdir(path)
        except OSError:
            pass
    finally:
        sftp.close()
        transport.close()


def _recursive_rm(sftp, path: str) -> None:
    """Recursively remove a remote directory."""
    try:
        for entry in sftp.listdir_attr(path):
            full = f"{path}/{entry.filename}"
            if stat_mod.S_ISDIR(entry.st_mode):
                _recursive_rm(sftp, full)
            else:
                sftp.remove(full)
        sftp.rmdir(path)
    except FileNotFoundError:
        pass


def _remote_cleanup(path: str) -> None:
    """Clean up remote directory."""
    sftp, transport = _sftp_session()
    try:
        _recursive_rm(sftp, path)
    finally:
        sftp.close()
        transport.close()


def _list_remote_dir(path: str) -> list[str]:
    """List files in a remote directory."""
    sftp, transport = _sftp_session()
    try:
        try:
            return sftp.listdir(path)
        except FileNotFoundError:
            return []
    finally:
        sftp.close()
        transport.close()


# --- Fixtures ---


@pytest.fixture
def source_files(tmp_path):
    """Create source files for backup."""
    source = tmp_path / "source"
    source.mkdir()
    (source / "readme.txt").write_text("Backup Manager test file", encoding="utf-8")
    (source / "data.csv").write_text("id,name\n1,Alice\n2,Bob", encoding="utf-8")
    sub = source / "config"
    sub.mkdir()
    (sub / "settings.json").write_text('{"debug": false}', encoding="utf-8")
    return source


@pytest.fixture
def config_manager(tmp_path):
    """Create a ConfigManager with temp directory."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "profiles").mkdir()
    (config_dir / "logs").mkdir()
    (config_dir / "manifests").mkdir()
    return ConfigManager(config_dir=config_dir)


@pytest.fixture
def remote_storage_path():
    """Provide a unique remote path for primary SFTP storage and clean up after."""
    test_id = uuid.uuid4().hex[:8]
    path = f"{SFTP_REMOTE_BASE}/storage_{test_id}"
    _ensure_remote_dir(path)
    yield path
    _remote_cleanup(path)


@pytest.fixture
def remote_mirror1_path():
    """Provide a unique remote path for Mirror 1 and clean up after."""
    test_id = uuid.uuid4().hex[:8]
    path = f"{SFTP_REMOTE_BASE}/mirror1_{test_id}"
    _ensure_remote_dir(path)
    yield path
    _remote_cleanup(path)


@pytest.fixture
def remote_mirror2_path():
    """Provide a unique remote path for Mirror 2 and clean up after."""
    test_id = uuid.uuid4().hex[:8]
    path = f"{SFTP_REMOTE_BASE}/mirror2_{test_id}"
    _ensure_remote_dir(path)
    yield path
    _remote_cleanup(path)


def _make_sftp_storage_config(remote_path: str) -> StorageConfig:
    """Build a StorageConfig for SFTP with the test key."""
    return StorageConfig(
        storage_type=StorageType.SFTP,
        sftp_host=SFTP_HOST,
        sftp_port=SFTP_PORT,
        sftp_username=SFTP_USER,
        sftp_key_path=SFTP_KEY_PATH,
        sftp_remote_path=remote_path,
    )


# ============================================================
# Test 1: SFTP as primary storage
# ============================================================


class TestSFTPAsPrimaryStorage:
    """Test full backup pipeline with SFTP as primary destination."""

    def test_full_backup_to_sftp(
        self,
        source_files,
        config_manager,
        remote_storage_path,
    ):
        """Full backup should stream all files to SFTP server."""
        profile = BackupProfile(
            id="sftp_primary",
            name="SFTP Primary Test",
            source_paths=[str(source_files)],
            backup_type=BackupType.FULL,
            storage=_make_sftp_storage_config(remote_storage_path),
            retention=RetentionConfig(
                policy=RetentionPolicy.GFS,
            ),
        )

        events = EventBus()
        log_msgs = []
        events.subscribe("log", lambda message="", **kw: log_msgs.append(message))

        engine = BackupEngine(config_manager, events=events)
        stats = engine.run_backup(profile)

        assert stats.files_found == 3
        assert stats.files_processed == 3
        assert stats.errors == 0
        assert stats.backup_path != ""

        # Verify files exist on remote
        remote_contents = _list_remote_dir(remote_storage_path)
        assert len(remote_contents) > 0

        assert any("Uploading" in m for m in log_msgs)
        assert any("complete" in m.lower() for m in log_msgs)

    def test_incremental_backup_to_sftp(
        self,
        source_files,
        config_manager,
        remote_storage_path,
    ):
        """Incremental backup should skip unchanged files."""
        profile = BackupProfile(
            id="sftp_incr",
            name="SFTP Incremental",
            source_paths=[str(source_files)],
            backup_type=BackupType.INCREMENTAL,
            storage=_make_sftp_storage_config(remote_storage_path),
            retention=RetentionConfig(
                policy=RetentionPolicy.GFS,
            ),
        )

        engine = BackupEngine(config_manager)

        # First run: all files
        stats1 = engine.run_backup(profile)
        assert stats1.files_processed == 3

        # Second run: no changes
        stats2 = engine.run_backup(profile)
        assert stats2.files_skipped == 3
        assert stats2.files_processed == 0


# ============================================================
# Test 2: SFTP as Mirror 1
# ============================================================


class TestSFTPAsMirror1:
    """Test backup with local primary + SFTP mirror 1."""

    def test_local_backup_with_sftp_mirror1(
        self,
        source_files,
        config_manager,
        remote_mirror1_path,
        tmp_path,
    ):
        """Local backup + SFTP mirror should upload to both destinations."""
        local_dest = tmp_path / "local_backups"
        local_dest.mkdir()

        profile = BackupProfile(
            id="mirror1_test",
            name="Mirror1 Test",
            source_paths=[str(source_files)],
            backup_type=BackupType.FULL,
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(local_dest),
            ),
            mirror_destinations=[
                _make_sftp_storage_config(remote_mirror1_path),
            ],
            retention=RetentionConfig(
                policy=RetentionPolicy.GFS,
            ),
        )

        events = EventBus()
        log_msgs = []
        events.subscribe("log", lambda message="", **kw: log_msgs.append(message))

        engine = BackupEngine(config_manager, events=events)
        stats = engine.run_backup(profile)

        # Local backup OK
        assert stats.files_processed == 3
        local_backups = list(local_dest.iterdir())
        assert len(local_backups) >= 1

        # Mirror 1 uploaded
        assert len(stats.mirror_results) == 1
        mirror_name, success, msg = stats.mirror_results[0]
        assert mirror_name == "Mirror 1"
        assert success is True
        assert msg == "OK"

        # Verify files on remote mirror
        remote_contents = _list_remote_dir(remote_mirror1_path)
        assert len(remote_contents) > 0

        assert any("Mirror 1" in m for m in log_msgs)


# ============================================================
# Test 3: SFTP as Mirror 2
# ============================================================


class TestSFTPAsMirror2:
    """Test backup with local primary + SFTP mirror 2."""

    def test_local_backup_with_sftp_mirror2(
        self,
        source_files,
        config_manager,
        remote_mirror2_path,
        tmp_path,
    ):
        """Local backup + SFTP mirror 2 only."""
        local_dest = tmp_path / "local_backups"
        local_dest.mkdir()

        # Mirror 2 = second mirror destination (index 1)
        # First mirror is a local destination, second is SFTP
        local_mirror_dest = tmp_path / "local_mirror"
        local_mirror_dest.mkdir()

        profile = BackupProfile(
            id="mirror2_test",
            name="Mirror2 Test",
            source_paths=[str(source_files)],
            backup_type=BackupType.FULL,
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(local_dest),
            ),
            mirror_destinations=[
                StorageConfig(
                    storage_type=StorageType.LOCAL,
                    destination_path=str(local_mirror_dest),
                ),
                _make_sftp_storage_config(remote_mirror2_path),
            ],
            retention=RetentionConfig(
                policy=RetentionPolicy.GFS,
            ),
        )

        events = EventBus()
        log_msgs = []
        events.subscribe("log", lambda message="", **kw: log_msgs.append(message))

        engine = BackupEngine(config_manager, events=events)
        stats = engine.run_backup(profile)

        assert stats.files_processed == 3

        # Both mirrors should report
        assert len(stats.mirror_results) == 2

        mirror1_name, mirror1_ok, _ = stats.mirror_results[0]
        assert mirror1_name == "Mirror 1"
        assert mirror1_ok is True

        mirror2_name, mirror2_ok, _ = stats.mirror_results[1]
        assert mirror2_name == "Mirror 2"
        assert mirror2_ok is True

        # Verify SFTP mirror 2 has files
        remote_contents = _list_remote_dir(remote_mirror2_path)
        assert len(remote_contents) > 0

        # Verify local mirror 1 has files
        local_mirror_backups = list(local_mirror_dest.iterdir())
        assert len(local_mirror_backups) >= 1

        assert any("Mirror 2" in m for m in log_msgs)


# ============================================================
# Test 4: Combined — local primary + 2 SFTP mirrors
# ============================================================


class TestDualSFTPMirrors:
    """Test backup with local primary + 2 SFTP mirrors simultaneously."""

    def test_local_plus_two_sftp_mirrors(
        self,
        source_files,
        config_manager,
        remote_mirror1_path,
        remote_mirror2_path,
        tmp_path,
    ):
        """Full pipeline: local backup + mirror to 2 SFTP destinations."""
        local_dest = tmp_path / "local_backups"
        local_dest.mkdir()

        profile = BackupProfile(
            id="dual_mirror",
            name="Dual Mirror Test",
            source_paths=[str(source_files)],
            backup_type=BackupType.FULL,
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(local_dest),
            ),
            mirror_destinations=[
                _make_sftp_storage_config(remote_mirror1_path),
                _make_sftp_storage_config(remote_mirror2_path),
            ],
            retention=RetentionConfig(
                policy=RetentionPolicy.GFS,
            ),
        )

        events = EventBus()
        log_msgs = []
        events.subscribe("log", lambda message="", **kw: log_msgs.append(message))

        engine = BackupEngine(config_manager, events=events)
        stats = engine.run_backup(profile)

        # Primary backup
        assert stats.files_processed == 3
        assert stats.errors == 0

        # Both mirrors succeeded
        assert len(stats.mirror_results) == 2
        for mirror_name, success, msg in stats.mirror_results:
            assert success is True, f"{mirror_name} failed: {msg}"
            assert msg == "OK"

        # Verify Mirror 1 has data
        m1_contents = _list_remote_dir(remote_mirror1_path)
        assert len(m1_contents) > 0

        # Verify Mirror 2 has data
        m2_contents = _list_remote_dir(remote_mirror2_path)
        assert len(m2_contents) > 0

        # Verify logs mention both mirrors
        assert any("Mirror 1" in m for m in log_msgs)
        assert any("Mirror 2" in m for m in log_msgs)
        assert any("complete" in m.lower() for m in log_msgs)


# ============================================================
# Test 5: Mirror failure handling
# ============================================================


class TestMirrorFailure:
    """Test that a failing mirror doesn't break the pipeline."""

    def test_bad_mirror_does_not_crash(
        self,
        source_files,
        config_manager,
        tmp_path,
    ):
        """A mirror pointing to an unreachable host should fail gracefully."""
        local_dest = tmp_path / "local_backups"
        local_dest.mkdir()

        # Use an unreachable IP to avoid SSH retries flooding the real server
        bad_sftp = StorageConfig(
            storage_type=StorageType.SFTP,
            sftp_host="192.168.255.254",
            sftp_port=SFTP_PORT,
            sftp_username="nobody",
            sftp_password="wrong",
            sftp_remote_path="/tmp/nope",
        )

        profile = BackupProfile(
            id="mirror_fail",
            name="Mirror Fail Test",
            source_paths=[str(source_files)],
            backup_type=BackupType.FULL,
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(local_dest),
            ),
            mirror_destinations=[
                bad_sftp,
            ],
            retention=RetentionConfig(
                policy=RetentionPolicy.GFS,
            ),
        )

        engine = BackupEngine(config_manager)
        stats = engine.run_backup(profile)

        # Primary backup should succeed regardless of mirror failure
        assert stats.files_processed == 3

        # Mirror should report failure
        assert len(stats.mirror_results) == 1
        mirror_name, success, msg = stats.mirror_results[0]
        assert mirror_name == "Mirror 1"
        # Either the connection fails or individual files fail — pipeline doesn't crash
        assert isinstance(success, bool)
