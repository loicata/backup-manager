# Backup Manager v3

[![CI](https://github.com/loicata/backup-manager/actions/workflows/ci.yml/badge.svg)](https://github.com/loicata/backup-manager/actions/workflows/ci.yml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/badge/tests-837%20passed-brightgreen.svg)](#testing)
[![Coverage](https://img.shields.io/badge/coverage-84%25-brightgreen.svg)](#testing)
[![Platform](https://img.shields.io/badge/platform-Windows%2010%2F11-0078D6.svg)](https://github.com/loicata/backup-manager/releases)

A production-grade, security-focused Windows backup application built for personal and small-business use. Multi-destination backups with end-to-end AES-256-GCM encryption, automated scheduling, and Grandfather-Father-Son retention.

| Profile configuration | Backup in progress |
|:---------------------:|:------------------:|
| ![General tab](assets/screenshots/general_tab.png) | ![Run backup](assets/screenshots/run_backup.png) |

---

## Highlights

| | Feature | Details |
|---|---------|---------|
| **4** | **Storage backends** | Local/USB, Network (UNC), SFTP (SSH), S3-compatible cloud |
| **+2** | **Mirror copies** | Independent replication with per-destination encryption |
| **AES-256** | **Streaming encryption** | GCM authenticated, no plaintext on disk (`.tar.wbenc`) |
| **GFS** | **Retention rotation** | Grandfather-Father-Son (daily/weekly/monthly) |
| **SHA-256** | **Integrity verification** | Pre-backup manifest + post-write check + periodic audits |
| **DPAPI** | **Password protection** | Windows user-bound, never in plaintext |

---

## Quick Start

### Install

Download **[BackupManager.msi](https://github.com/loicata/backup-manager/releases/latest)** and run it. That's it.

### First launch

A 3-step wizard creates your first profile:

1. **Name** your backup
2. **Pick source folders** to protect
3. **Choose a destination** (USB, network, SFTP, or S3)

Click **Finish** — daily backups are enabled by default.

### From source

```bash
git clone https://github.com/loicata/backup-manager.git
cd backup-manager
pip install -r requirements.txt
python -m src
```

---

## Features

### Multi-profile management

- Unlimited profiles with independent sources, destinations, schedule, encryption, and retention.
- **Active / Inactive** profiles — inactive profiles are paused but preserved.
- Reorder with Up/Down controls; switch in a single click.
- Configuration validated before every backup with clear error messages.

### Storage & Destinations

| Destination | Description |
|-------------|-------------|
| **Local / USB** | Any local drive, external HDD, or removable USB storage |
| **Network (UNC)** | Windows shared folders (`\\server\share`) with username/password |
| **SFTP (SSH)** | Remote servers with password or private key (Ed25519, ECDSA, RSA) |
| **S3 Cloud** | AWS, Scaleway, Wasabi, OVH, DigitalOcean, Cloudflare R2, Backblaze B2, MinIO |

### Mirrors (multi-destination replication)

- Up to **2 independent mirror copies** in addition to the primary destination.
- Each mirror can use a **different storage type** and **independent encryption settings** (e.g. primary on USB unencrypted, Mirror 1 on SFTP encrypted, Mirror 2 on S3 encrypted).
- Mirrors execute automatically after each successful primary write.
- Mirror failures are reported independently — the primary backup is never affected.
- GFS rotation is applied independently on each destination.

### Backup modes

| Mode | Description |
|------|-------------|
| **Full** | Complete copy of all selected files. Self-contained restore point. |
| **Differential** | Only files changed since last full backup. SHA-256 manifest comparison. Configurable full backup cycle. |

### Retention (GFS rotation)

Grandfather-Father-Son rotation keeps backups organized and storage predictable:

- **Daily:** number of daily backups to keep beyond today.
- **Weekly:** number of weekly full backups to keep beyond the current week.
- **Monthly:** number of monthly full backups to keep beyond the current month.

Rotation is applied independently on each destination (primary + mirrors).

### Scheduling & Reliability

- **Manual, Hourly, Daily, Weekly, or Monthly** via Windows Task Scheduler.
- **Auto-start at logon** for unattended operation.
- **Retry on failure** with progressive delays: 2, 10, 30, 90, and 240 minutes.
- **Pre-backup target check** — all destinations verified before backup starts.
- **System tray** mode for silent background operation.
- **Missed backup detection** — runs automatically on next startup.
- **Schedule journal** — all backups logged with profile, status, and details.

### Recovery

- **Local restore** — browse a backup folder or select an encrypted `.tar.wbenc` file.
- **Remote retrieve** — download from SFTP or S3 directly from the Recovery tab.
- **Automatic decryption** — encrypted archives decrypted on-the-fly.
- **Long path support** — Windows 260-character limit handled transparently.

### Periodic Integrity Verification

- **On-demand** — verify all backups across all destinations from the Verify tab.
- **Scheduled** — automatic periodic verification (default: every 7 days).
- **Email reports** — structured HTML table with results per destination and backup.

### Email notifications

- SMTP alerts on backup **success** or **failure**.
- HTML-formatted reports with file count, duration, destination, errors.
- Provider presets for Gmail, Outlook, Yahoo.

### Main interface

| Tab | Description |
|-----|-------------|
| **Run** | Launch backup, view real-time progress and logs |
| **General** | Profile name, backup type, source folders, exclusion patterns |
| **Storage** | Primary destination type and connection settings |
| **Mirror 1 / 2** | Optional mirror destinations |
| **Encryption** | AES-256-GCM toggle per destination with password management |
| **Schedule** | Frequency, time, auto-retry, periodic verification, journal |
| **Retention** | GFS rotation policy (daily, weekly, monthly counts) |
| **Email** | SMTP settings with provider presets and test button |
| **Recovery** | Restore from local or retrieve from remote |
| **Verify** | On-demand integrity verification with real-time results |
| **History** | Browse past backup logs |

---

## Security Architecture

Defense-in-depth model with multiple independent security layers. Even if an attacker gains access to the backup storage, the data remains unreadable without the encryption password.

### Encryption at rest — `.tar.wbenc` streaming format

No plaintext data is ever written to disk:

```
.tar.wbenc file layout:

Header (37 bytes):
  [4B magic: "WBEC"]        — file format identifier
  [1B version: 0x01]        — format version
  [16B salt]                 — random salt for key derivation
  [16B reserved]             — future use (zeroed)

Body (repeating chunks):
  [4B plaintext_length]     — big-endian chunk size
  [12B nonce]                — sequential counter (never reused)
  [ciphertext + 16B GCM tag] — authenticated encrypted data

EOF sentinel:
  [4B zeros]                 — marks end of stream
```

1. Source files are streamed into a tar archive in memory.
2. The tar stream is split into 1 MB chunks.
3. Each chunk is encrypted independently with AES-256-GCM before writing to disk.
4. The integrity manifest (`.wbverify`) is embedded inside the archive.
5. The original plaintext files are never written to the destination.

### Cipher and key derivation

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| **Cipher** | AES-256-GCM | NIST-approved authenticated encryption |
| **Key size** | 256 bits | Maximum AES key length |
| **Nonce** | 12 bytes, sequential counter | Unique per chunk, prevents reuse |
| **Authentication tag** | 16 bytes (128 bits) | Detects tampering or corruption |
| **Key derivation** | PBKDF2-HMAC-SHA256 | Industry-standard password-based KDF |
| **Iterations** | 600,000 | OWASP 2024 recommendation |
| **Salt** | 16 random bytes | `os.urandom()`, prevents rainbow tables |

### Password storage

| Platform | Method | Details |
|----------|--------|---------|
| **Windows** | DPAPI (`CryptProtectData`) | Tied to current Windows user account |
| **Fallback** | AES-256-GCM with machine key | DPAPI-protected 32-byte machine key |

- Minimum password length: **16 characters**.
- Passwords **never logged**, **never in plaintext files**, **never in error messages**.

### Secure memory handling

- Passwords wrapped in `SecurePassword` context manager during pipeline execution.
- `bytearray` buffers explicitly **zeroed after use**.
- Derived keys zeroed immediately after encryption/decryption.

### Integrity verification pipeline

1. **Pre-backup manifest** — SHA-256 hash of every source file.
2. **Post-write verification** — re-read and re-hash against manifest.
3. **Remote verification** — SFTP: server-side `sha256sum`; S3: ETag comparison.
4. **GCM authentication** — per-chunk tamper detection.
5. **Zero-tolerance** — any mismatch marks the backup as **failed**.

### Transport security

| Transport | Protection |
|-----------|------------|
| **SFTP** | SSH encrypted channel, host key verification (TOFU) |
| **S3** | HTTPS/TLS, AWS Signature V4 |
| **Network** | Windows SMB, DPAPI credentials |
| **Local** | Direct filesystem access |

### Security summary

| Layer | Mechanism |
|-------|-----------|
| **Data at rest** | AES-256-GCM streaming (`.tar.wbenc`) |
| **Key derivation** | PBKDF2-HMAC-SHA256, 600K iterations, random salt |
| **Password storage** | Windows DPAPI (user-bound) + AES-256-GCM fallback |
| **Integrity** | SHA-256 manifest + post-write verify + GCM auth |
| **Transport** | SSH / HTTPS / SMB |
| **Memory** | Explicit buffer zeroing |
| **Path safety** | Traversal-proof remote path validation |
| **Logging** | No secrets in any log output |

---

## Testing

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=src --cov-report=term-missing

# Run a specific test file
pytest tests/unit/test_hashing.py -v
```

**Current status:** 837 tests | 84% coverage | 0 failures

CI pipeline: GitHub Actions on every push — Black formatting, Ruff linting (Ubuntu), full test suite with coverage enforcement (Windows, Python 3.12 + 3.13).

---

## Build from Source

### Prerequisites

- Python 3.11+ (tested on 3.12 and 3.13)
- [WiX Toolset v3.14](https://wixtoolset.org/) (for MSI packaging only)

### Build the executable

```bash
python build_pyinstaller.py
```

Output: `dist/BackupManager/BackupManager.exe`

### Build the MSI installer

```bash
python build_msi.py
```

Output: `dist/BackupManager-x.y.z.msi`

---

## Project Structure

```
backup-manager/
├── src/
│   ├── core/                     # Backup engine, scheduler, config, pipeline
│   │   ├── backup_engine.py         # Main orchestrator (11-phase pipeline)
│   │   ├── config.py                # Profile dataclasses & JSON persistence
│   │   ├── events.py                # Thread-safe event bus for UI updates
│   │   ├── integrity_verifier.py    # Periodic backup integrity verification
│   │   ├── scheduler.py             # Windows Task Scheduler + in-app scheduler
│   │   └── phases/                  # Pipeline phases
│   │       ├── collector.py            # File collection & exclusion filtering
│   │       ├── filter.py               # Differential change detection
│   │       ├── encryptor.py            # Streaming tar encryption
│   │       ├── writer.py               # Write dispatcher (local/remote)
│   │       ├── verifier.py             # Post-write integrity verification
│   │       ├── mirror.py               # Mirror replication orchestrator
│   │       └── rotator.py              # GFS retention rotation
│   ├── storage/                  # Storage backends
│   │   ├── local.py, network.py, sftp.py, s3.py
│   │   └── base.py                  # Abstract backend + retry decorator
│   ├── security/                 # Encryption, DPAPI, secure memory
│   ├── notifications/            # SMTP email with HTML reports
│   └── ui/                       # Tkinter GUI (Sun Valley theme)
├── tests/                        # 837 tests (unit + integration)
├── CHANGELOG.md                  # Release history
├── requirements.txt              # Runtime dependencies
└── pyproject.toml                # Project metadata & tool config
```

---

## Requirements

| Requirement | Version |
|-------------|---------|
| **OS** | Windows 10 / 11 |
| **Python** | 3.11+ (development only — end users install the MSI) |
| **cryptography** | >= 43.0.0 |
| **paramiko** | >= 3.0.0 |
| **boto3** | >= 1.35.0 |
| **Pillow** | >= 10.0.0 |
| **pystray** | >= 0.19.0 |
| **sv_ttk** | >= 2.6.0 |

---

## License

[GNU General Public License v3.0](LICENSE) — Copyright (c) 2026 Loic Ader [loicata.com](https://loicata.com)

---

## Contributing

Contributions are welcome. Please open an issue before submitting a pull request for any significant change.
