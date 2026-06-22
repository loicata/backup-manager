"""Stage 5 cleanup: remove the corrupt 2026-05-14 backup remnants.

The Stage 5 trial run uploaded a tar stream that the server helper
silently dropped on the floor (POSIX-sh vs bash mismatch on dash --
``[[`` not supported). The backup directory ended up empty but BM
marked the run as successful via the .wbcommit file. This script:

1. Refuses to run if BackupManager.exe is alive (race risk).
2. Deletes the 4 stale server-side entries for the broken run.
3. Rewrites the local profile JSON so last_backup_* point back to the
   genuine 2026-05-13 backup (which is intact, 43 GB, 231908 files).
4. Reports what changed.

Safe-by-default: prints the plan and waits for ``--yes`` before
touching anything. The 2026-05-13 backup is never touched.

Run from repo root after closing BackupManager:
    .venv/Scripts/python.exe scripts/cleanup-stage5.py --yes
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from src.core.config import ConfigManager
from src.storage.sftp import SFTPStorage

PROFILE_ID = "7a9eb786a81f49a293fbbb96be8e5197"
CORRUPT_BACKUP = "My_Backup_FULL_2026-05-14_103508"

# Values taken from the user's profile before the corrupt run ran.
# Captured during the initial diagnostic — restoring them puts BM in
# the same state as "the 13/05 run is the latest successful one".
LAST_GOOD_BACKUP = "2026-05-13T22:04:28.782922"
LAST_GOOD_FULL = "2026-05-13T22:04:19.399568"

# Suffixes attached to the backup directory name on the SFTP server.
# Order matters for cosmetics only — the SFTP backend deletes them
# independently.
SERVER_SUFFIXES = ["", ".wbcommit", ".wbverify", ".wbserverhashes"]


def _check_bm_not_running() -> None:
    """Refuse to proceed if BackupManager.exe is alive.

    The scheduler can fire a backup at any moment; mutating the profile
    JSON or the server state mid-run would corrupt things further.
    """
    import subprocess

    result = subprocess.run(
        ["tasklist", "/FI", "IMAGENAME eq BackupManager.exe"],
        capture_output=True,
        text=True,
        check=False,
    )
    if "BackupManager.exe" in result.stdout:
        print("ERROR: BackupManager.exe is still running. Close it first.", file=sys.stderr)
        sys.exit(2)


def _delete_server_remnants(profile, dry_run: bool) -> None:
    s = profile.storage
    backend = SFTPStorage(
        host=s.sftp_host,
        port=s.sftp_port,
        username=s.sftp_username,
        password=s.sftp_password,
        key_path=s.sftp_key_path,
        key_passphrase=s.sftp_key_passphrase,
        remote_path=s.sftp_remote_path,
    )
    backend.connect()
    try:
        for suffix in SERVER_SUFFIXES:
            name = CORRUPT_BACKUP + suffix
            print(f"  {'DRY' if dry_run else 'DEL'} -- {s.sftp_remote_path}/{name}")
            if not dry_run:
                try:
                    backend.delete_backup(name)
                except FileNotFoundError as e:
                    print(f"       (already gone: {e})")
                except OSError as e:
                    print(f"       FAILED: {e}")
    finally:
        backend.disconnect()


def _fix_local_profile(dry_run: bool) -> None:
    cm = ConfigManager()
    path = cm.config_dir / "profiles" / f"{PROFILE_ID}.json"
    raw = json.loads(path.read_text(encoding="utf-8"))

    before = {
        "last_backup": raw.get("last_backup"),
        "last_full_backup": raw.get("last_full_backup"),
        "last_backup_completed": raw.get("last_backup_completed"),
    }
    print(f"  Local profile {path.name}:")
    for k, v in before.items():
        print(f"    {k} = {v!r}")

    raw["last_backup"] = LAST_GOOD_BACKUP
    raw["last_full_backup"] = LAST_GOOD_FULL
    raw["last_backup_completed"] = True
    # Wipe the recovery markers so BM does not try to "resume" the
    # corrupt run on startup.
    raw["incomplete_backup_name"] = ""
    raw["incomplete_backup_was_full"] = False
    raw["crash_recovery_attempts"] = 0

    print(f"  After fix:")
    print(f"    last_backup = {raw['last_backup']!r}")
    print(f"    last_full_backup = {raw['last_full_backup']!r}")
    print(f"    incomplete_backup_name = '' (cleared)")

    if not dry_run:
        path.write_text(json.dumps(raw, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  Wrote {path}")


def main() -> int:
    dry_run = "--yes" not in sys.argv

    if dry_run:
        print("=== DRY RUN -- pass --yes to actually delete/edit ===")
    else:
        print("=== EXECUTING CLEANUP ===")
        _check_bm_not_running()

    cm = ConfigManager()
    profile_file = cm.config_dir / "profiles" / f"{PROFILE_ID}.json"
    profile = cm._load_profile_file(profile_file)

    print()
    print(f"Profile: {profile.name}")
    print(f"Server:  {profile.storage.sftp_username}@{profile.storage.sftp_host}")
    print()
    print("Step 1: delete corrupt server-side entries")
    _delete_server_remnants(profile, dry_run)
    print()
    print("Step 2: restore local profile timestamps")
    _fix_local_profile(dry_run)
    print()
    if dry_run:
        print("Re-run with --yes to apply.")
    else:
        print("Cleanup complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
