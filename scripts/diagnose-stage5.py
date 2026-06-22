"""Stage 5 diagnostic: reproduce the list_backup_files() call BM made.

Loads the 'My Backup' profile (decrypts DPAPI secrets the same way the
running app does), connects via SFTPStorage, and calls list_backup_files
on today's backup directory. The result tells us whether the empty
list seen in the log is a server-side issue (find returns nothing) or
a client-side issue (Python parsing/transport state).

Output:
    - "N files" with N > 0 -> server is fine, bug is in BM's transport
      reuse after _upload_tar_stream_with_helper
    - "0 files" -> server-side problem (perms, missing dir, bad find)
    - exception -> connection/auth issue

Run from repo root:
    .venv/Scripts/python.exe scripts/diagnose-stage5.py
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from src.core.config import ConfigManager
from src.storage.sftp import SFTPStorage

PROFILE_ID = "7a9eb786a81f49a293fbbb96be8e5197"
BACKUP_NAME = "My_Backup_FULL_2026-05-14_103508"


def main() -> int:
    cm = ConfigManager()
    profile_file = cm.config_dir / "profiles" / f"{PROFILE_ID}.json"
    if not profile_file.exists():
        print(f"ERROR: profile not found: {profile_file}")
        return 1

    profile = cm._load_profile_file(profile_file)
    s = profile.storage

    print(f"Profile: {profile.name}")
    print(f"Target:  {s.sftp_username}@{s.sftp_host}:{s.sftp_port}{s.sftp_remote_path}")
    print(f"Backup:  {BACKUP_NAME}")
    print()

    backend = SFTPStorage(
        host=s.sftp_host,
        port=s.sftp_port,
        username=s.sftp_username,
        password=s.sftp_password,
        key_path=s.sftp_key_path,
        key_passphrase=s.sftp_key_passphrase,
        remote_path=s.sftp_remote_path,
    )

    print("Connecting...")
    backend.connect()
    try:
        transport = backend._get_transport()
        full_path = backend._join_remote(BACKUP_NAME)
        print(f"Resolved remote path: {full_path}")
        print()

        for cmd, label in [
            ("ls -la /home/cipango56/backups/ 2>&1", "backups dir after cleanup"),
            ("ls /home/cipango56/backups/My_Backup_FULL_2026-05-14_103508* 2>&1 || echo '(none)'",
             "14/05 remnants (should be empty)"),
        ]:
            print(f"--- {label} ---")
            print(f"$ {cmd}")
            channel = transport.open_session()
            try:
                channel.settimeout(60)
                channel.exec_command(cmd)
                out = b""
                while True:
                    chunk = channel.recv(65536)
                    if not chunk:
                        break
                    out += chunk
                exit_status = channel.recv_exit_status()
                print(f"  exit={exit_status}")
                print(f"  stdout: {out.decode('utf-8', errors='replace').strip()[:500]}")
            finally:
                channel.close()
            print()
    finally:
        backend.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
