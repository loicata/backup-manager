# Backup Manager v3.1.5

A reliable, secure, and user-friendly Windows backup application designed for personal and small-business use. Manage multiple backup profiles, store copies on local drives, network shares, or remote servers, and let the built-in scheduler and GFS retention policy take care of the rest.

---

## Screenshots

| Profile configuration | Backup in progress |
|:---------------------:|:------------------:|
| ![General tab](assets/screenshots/general_tab.png) | ![Run backup](assets/screenshots/run_backup.png) |

---

## Key Features

### Multi-profile management

- Create as many backup profiles as you need, each with its own sources, destination, schedule, and retention settings.
- Profiles can be **active** (run automatically on schedule) or **inactive** (paused, kept for later use).
- Reorder profiles with Up/Down buttons; switch between them in a single click.

### Storage & Destinations

| Destination | Description |
|-------------|-------------|
| **Local / USB** | Backup to any local drive, external HDD, or USB stick |
| **Network (UNC)** | Backup to shared network folders (`\\server\share`) |
| **SFTP (SSH)** | Backup to any remote server with password or SSH key authentication |
| **S3 Cloud** | Backup to Amazon S3 or any S3-compatible provider |

### Mirrors

- Up to **2 additional copies** on independent destinations.
- Each mirror can use a different storage type (e.g. primary on USB, Mirror 1 on SFTP, Mirror 2 on S3).
- Mirrors run automatically after each successful primary backup.

### Backup modes

| Mode | Description |
|------|-------------|
| **Full** | Complete copy of all selected files every time |
| **Differential** | Only files changed since the last full backup |

### Retention (GFS rotation)

Grandfather-Father-Son rotation keeps backups organized and storage usage under control:

- **Daily:** number of daily backups to keep beyond today
- **Weekly:** number of weekly backups to keep beyond the current week
- **Monthly:** number of monthly backups to keep beyond the current month

Old backups are automatically deleted when the configured limits are exceeded. The UI displays user-friendly values (today's backup is always kept on top of the configured count).

### Encryption

- **AES-256-GCM** encryption with per-destination control.
- Encrypt the primary destination, Mirror 1, and/or Mirror 2 independently.
- Passwords stored securely via **Windows DPAPI** (or AES-256-GCM fallback on non-Windows systems).
- Key derivation: **PBKDF2-HMAC-SHA256** with 600,000 iterations (OWASP 2024 recommendation).
- Minimum password length: **16 characters**.

### Scheduling & Reliability

- **Hourly, Daily, or Weekly** automatic scheduling via Windows Task Scheduler.
- **Auto-start at logon** option for unattended operation.
- **Retry on failure** with progressive delays: 2, 10, 30, 90, and 240 minutes.
- **Pre-backup target check:** all configured destinations (storage + mirrors) are verified before backup starts. If any target is unreachable, the user is prompted to connect it or cancel.
- **System tray** mode for silent background operation.

### Integrity & Verification

- **SHA-256 checksum manifest** generated before each backup.
- **Post-backup verification** re-reads and re-hashes every file to confirm integrity.
- **Remote verification:** SHA-256 hash check via server-side `sha256sum` (SFTP), ETag/MD5 comparison (S3).
- **Zero-tolerance policy:** any missing or corrupted file marks the entire backup as failed.
- `.wbverify` manifest saved alongside each backup for future auditing.

### Recovery

- **Restore** from any local backup folder with optional decryption.
- **Retrieve** backups stored on remote servers (SFTP, S3) directly from the Recovery tab — no external tool required.

### Email notifications
- SMTP-based email alerts on backup success or failure.
- Configurable recipient, subject, and server settings.
- HTML-formatted reports with backup details.

### History

- Complete log of all backups with date, profile name, and size.
- Quick overview of backup activity across all profiles.

---

## Installation

### MSI Installer (recommended)

1. Download `BackupManager.msi` from the [Releases](https://github.com/loicata/backup-manager/releases) page.
2. Run the installer and follow the wizard.
3. Launch Backup Manager from the desktop shortcut or Start Menu.
4. The application launches automatically after installation.

### From Source

```bash
git clone https://github.com/loicata/backup-manager.git
cd backup-manager

pip install -r requirements.txt

python -m src
```

---

## Quick Start

### Setup Wizard (first run)

On first launch, a 3-step wizard guides you through the essential configuration:

1. **Profile name** — give your backup a meaningful name (e.g. "My Documents", "Work Projects").
2. **What to back up?** — add one or more source folders to include in the backup.
3. **Where to store?** — choose a primary storage destination:
   - External drive / USB stick
   - Network folder (UNC path)
   - Remote server via SFTP (SSH)
   - S3 Cloud Storage (AWS, Scaleway, Wasabi, OVH, DigitalOcean, Cloudflare, Backblaze)

Click **Finish** — the wizard creates your profile with a daily schedule enabled by default.

### Main interface

After the wizard, you land on the main interface. Before launching your first backup, you can fine-tune every aspect of the configuration through the following tabs:

| Tab | Description |
|-----|-------------|
| **Run** | Launch a backup manually, view real-time progress and logs |
| **General** | Profile name, backup type (Full / Differential), source folders, full backup cycle |
| **Storage** | Primary storage destination and connection settings |
| **Mirror 1** | First optional mirror destination (local, network, SFTP, or S3) |
| **Mirror 2** | Second optional mirror destination |
| **Encryption** | AES-256-GCM encryption per destination (primary, mirror 1, mirror 2) |
| **Schedule** | Backup frequency (manual, hourly, daily, weekly, monthly) and time |
| **Retention** | GFS rotation policy — how many daily, weekly, and monthly backups to keep |
| **Email** | Email notifications on backup success or failure (SMTP configuration) |
| **Recovery** | Restore a local backup or retrieve files from a remote destination |
| **History** | View past backup results and logs |

Click **Start backup** on the Run tab to perform an immediate backup, or let the scheduler handle it automatically.

---

## Build from Source

### Prerequisites

- Python 3.12 or 3.13
- [WiX Toolset v3.14](https://wixtoolset.org/) (for MSI packaging only)

### Build the executable

```bash
python -m PyInstaller BackupManager.spec
```

The output is in `dist/BackupManager/`.

### Build the MSI installer

```bash
cd dist
heat.exe dir BackupManager -ag -sfrag -srd -dr INSTALLFOLDER -cg ProductFiles -var var.SourceDir -out HeatFiles.wxs
candle.exe -dSourceDir=BackupManager Product.wxs HeatFiles.wxs -o obj/
light.exe obj/Product.wixobj obj/HeatFiles.wixobj -o BackupManager.msi -ext WixUIExtension -b BackupManager -sice:ICE38 -sice:ICE91 -sice:ICE64
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

**Current status:** 559 tests | 84% coverage | 0 failures

---

## Project Structure

```
backup-manager/
├── src/
│   ├── core/                  # Backup engine, scheduler, config, pipeline
│   │   ├── backup_engine.py      # Main orchestrator
│   │   ├── config.py             # Profile management & dataclasses
│   │   ├── scheduler.py          # Windows Task Scheduler integration
│   │   └── phases/               # Pipeline phases (collect, hash, write, verify, rotate, mirror)
│   ├── storage/                # Storage backends
│   │   ├── local.py               # Local / USB / UNC
│   │   ├── sftp.py                # SFTP via Paramiko
│   │   └── s3.py                  # Amazon S3 via Boto3
│   ├── security/               # Security layer
│   │   ├── encryption.py          # AES-256-GCM, DPAPI, password storage
│   │   └── secure_memory.py       # Secure memory handling
│   ├── notifications/          # Alerting
│   │   └── email_notifier.py      # SMTP notifications
│   └── ui/                     # GUI (Tkinter)
│       ├── app.py                 # Main application window
│       ├── wizard.py              # First-launch setup wizard
│       ├── theme.py               # Colors, fonts, layout constants
│       └── tabs/                  # UI tabs (Run, General, Storage, Mirror, Encryption, Schedule, Retention, Email, Recovery, History)
├── tests/
│   ├── unit/                   # Unit tests (~559 tests)
│   ├── integration/            # Integration tests
│   └── fixtures/               # Shared test data
├── assets/                     # Icons, license, launcher, screenshots
├── requirements.txt            # Python dependencies
├── pyproject.toml              # Project metadata
├── BackupManager.spec          # PyInstaller build spec
└── CLAUDE.md                   # AI assistant directives
```

---

## Security Model

| Layer | Mechanism |
|-------|-----------|
| **Passwords at rest** | Encrypted via Windows DPAPI (tied to current user account) |
| **Fallback encryption** | AES-256-GCM with random 32-byte machine key |
| **Key derivation** | PBKDF2-HMAC-SHA256, 600,000 iterations |
| **Machine key protection** | Stored in DPAPI-encrypted blob |
| **Password policy** | Minimum 16 characters |
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
---

## License

[GNU General Public License v3.0](LICENSE) — Copyright (c) 2026 Loic Ader

---

## Contributing

Contributions are welcome. Please open an issue before submitting a pull request for any significant change.
