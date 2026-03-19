# Backup Manager v3.0.1

A reliable, secure, and user-friendly Windows backup application designed for personal and small-business use. Supports local drives, network shares, and remote servers with encryption, scheduling, and intelligent retention.

---

## Key Features

### Storage & Destinations

| Feature | Description |
|---------|-------------|
| **Local / USB** | Backup to any local drive, external HDD, or USB stick |
| **Network (UNC)** | Backup to shared network folders (`\\server\share`) |
| **SFTP** | Backup to any SSH/SFTP server (password or key-based auth) |
| **S3** | Backup to Amazon S3 or compatible storage (beta) |
| **Proton Drive** | Backup to Proton Drive via rclone (beta) |
| **Mirrors** | Up to 2 additional copies on independent destinations |

### Backup Modes

| Mode | Description |
|------|-------------|
| **Full** | Complete copy of all selected files |
| **Incremental** | Only files changed since the last backup (beta) |
| **Differential** | Only files changed since the last full backup (beta) |

### Retention (GFS)

Grandfather-Father-Son rotation policy keeps your backups organized:
- **Daily:** keep the most recent backup of each day (default: 7 days)
- **Weekly:** keep one backup per week (default: 4 weeks)
- **Monthly:** keep one backup per month (default: 12 months)

Old backups are automatically deleted when the configured limits are exceeded.

### Encryption

- **AES-256-GCM** encryption with per-destination control
- Encrypt primary, Mirror 1, and/or Mirror 2 independently
- Passwords stored securely via **Windows DPAPI** (or AES-256-GCM fallback)
- Key derivation: PBKDF2-HMAC-SHA256 with 600,000 iterations (OWASP 2024)

### Scheduling & Reliability

- **Daily or Weekly** automatic scheduling via Windows Task Scheduler
- **Auto-start at logon** option
- **Retry on failure** with progressive delays: 2, 10, 30, 90, and 240 minutes
- **System tray** mode for background operation

### Integrity & Verification

- SHA-256 checksum manifest generated before each backup
- Post-backup verification re-reads and re-hashes every file
- `.wbverify` manifest saved alongside each backup for future auditing

### Email Notifications (beta)

- SMTP-based email alerts on backup success or failure
- Configurable recipient, subject, and server settings

---

## Screenshots

*Coming soon*

---

## Installation

### MSI Installer (recommended)

1. Download `BackupManager.msi` from the [Releases](https://github.com/loicata/backup-manager/releases) page
2. Run the installer and follow the wizard
3. Launch Backup Manager from the desktop shortcut or Start Menu

### From Source

```bash
git clone https://github.com/loicata/backup-manager.git
cd backup-manager

pip install -r requirements.txt

python -m src
```

---

## Quick Start

1. **Launch** Backup Manager — the Setup Wizard appears on first run
2. **Name** your backup profile
3. **Select sources** — folders or files to back up
4. **Choose destination** — local drive, network share, or remote server
5. **Configure mirrors** (optional) — add up to 2 additional destinations
6. **Set schedule** — daily or weekly with automatic retry
7. **Click Finish** — your first backup is ready to run

---

## Build from Source

### Prerequisites

- Python 3.12+
- [WiX Toolset v3.14](https://wixtoolset.org/) (for MSI packaging only)

### Build the executable

```bash
python -m PyInstaller BackupManager.spec
```

The output is in `dist/BackupManager/`.

### Build the MSI installer

```bash
python build_msi.py
```

The output is `dist/BackupManager.msi`.

---

## Testing

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=src --cov-report=term-missing

# Run a specific test file
pytest tests/unit/test_backup_engine.py -v
```

**Current status:** 492 tests | 90% coverage | 0 failures

---

## Project Structure

```
backup-manager/
├── src/
│   ├── core/               # Backup engine, scheduler, config, pipeline
│   │   ├── backup_engine.py    # Main orchestrator
│   │   ├── config.py           # Profile management & dataclasses
│   │   ├── scheduler.py        # Windows Task Scheduler integration
│   │   └── phases/             # Pipeline phases (collect, hash, write, verify, rotate, mirror)
│   ├── storage/             # Storage backends
│   │   ├── local.py            # Local / USB / UNC
│   │   ├── sftp.py             # SFTP via Paramiko
│   │   ├── s3.py               # Amazon S3 via Boto3
│   │   └── proton.py           # Proton Drive via rclone
│   ├── security/            # Security layer
│   │   ├── encryption.py       # AES-256-GCM, DPAPI, password storage
│   │   └── secure_memory.py    # Secure memory handling
│   ├── notifications/       # Alerting
│   │   └── email_notifier.py   # SMTP notifications
│   └── ui/                  # GUI
│       ├── app.py              # Main application window
│       ├── wizard.py           # First-launch setup wizard
│       └── tabs/               # UI tabs (General, Storage, Mirror, Schedule, etc.)
├── tests/
│   ├── unit/                # Unit tests (~490 tests)
│   ├── integration/         # Integration tests
│   └── fixtures/            # Shared test data
├── assets/                  # Icons, license, launcher
├── requirements.txt         # Python dependencies
├── pyproject.toml           # Project metadata
└── CLAUDE.md                # AI assistant directives
```

---

## Security Model

| Layer | Mechanism |
|-------|-----------|
| **Passwords at rest** | Encrypted via Windows DPAPI (tied to user account) |
| **Fallback encryption** | AES-256-GCM with random 32-byte machine key |
| **Key derivation** | PBKDF2-HMAC-SHA256, 600,000 iterations |
| **Machine key protection** | Stored in DPAPI-encrypted blob |
| **SFTP path safety** | Path traversal blocked (no `..` in remote paths) |
| **Memory handling** | Sensitive buffers zeroed after use |
| **No secrets in logs** | Passwords and keys never appear in log files |

---

## Requirements

| Requirement | Version |
|-------------|---------|
| **OS** | Windows 10 / 11 |
| **Python** | 3.12+ (development only) |
| **cryptography** | >= 43.0.0 |
| **paramiko** | >= 3.0.0 |
| **boto3** | >= 1.35.0 |
| **Pillow** | >= 10.0.0 |
| **pystray** | >= 0.19.0 |
| **pyotp** | >= 2.9.0 |

---

## License

[MIT License](LICENSE) — Copyright (c) 2026 Loic Ader

---

## Contributing

Contributions are welcome. Please open an issue before submitting a pull request for any significant change.
