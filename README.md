# Backup Manager v3

[![CI](https://github.com/loicata/backup-manager/actions/workflows/ci.yml/badge.svg)](https://github.com/loicata/backup-manager/actions/workflows/ci.yml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/badge/tests-1407%20passed-brightgreen.svg)](#testing)
[![Coverage](https://img.shields.io/badge/coverage-85%25-brightgreen.svg)](#testing)
[![Platform](https://img.shields.io/badge/platform-Windows%2010%2F11-0078D6.svg)](https://github.com/loicata/backup-manager/releases)

**A Windows backup application you do not need to be a technician to use.**

Designed for families, freelancers, and small businesses. You install the MSI, the wizard asks you a few questions in plain language, and backups run on their own from that point. No scripts, no command line, no server to configure.

What sets it apart from the usual "drag your files to a USB drive" routine: an optional **ransomware-proof mode** that stores backups with **Amazon AWS S3 Object Lock** — once a backup is uploaded, it cannot be deleted or altered for as long as you chose (4 months to 7 years, or a custom duration). Not by you, not by someone who steals your computer, not by a ransomware that encrypts everything it can reach.

Everything else — encryption, scheduling, email alerts, integrity checks — works in the background. You open the app when you want to restore a file or check a log.

---

## Who this is for

- **Families and individuals** who want a "set it and forget it" backup for photos, documents, and personal files.
- **Freelancers and micro-businesses** who cannot afford to lose accounting data, contracts, or client work to a ransomware incident.
- **Anyone who has been locked out of their own files** — or knows someone who has — and does not want to go through that again.

You do not need to know what SFTP, Object Lock, or PBKDF2 is. The wizard picks sane defaults, the interface hides the jargon, and the app refuses to start a run that would silently lose data.

For developers and ops engineers who want to look under the hood: everything is in the [Security architecture](#security-architecture) section below, with the full cipher / KDF / storage-format spec.

---

## Two modes, picked in 30 seconds at first launch

| Setup Wizard | Run Backup |
|:---:|:---:|
| <img src="assets/screenshots/wizard_mode_choice.png" width="450"> | <img src="assets/screenshots/run_backup.png" width="450"> |

| General Settings | Mirror Configuration |
|:---:|:---:|
| <img src="assets/screenshots/general_tab.png" width="450"> | <img src="assets/screenshots/mirror_tab.png" width="450"> |

### Classic — simple and fast

Backup to an external drive, a network share, an SSH server, or S3-compatible cloud storage. **Three-step wizard**: name it, pick the folders to save, pick where to save them. That's it. The app handles scheduling, integrity checks, and keeping old backups around based on a Grandfather-Father-Son policy.

Best for: you already have a backup drive or a NAS at home and you want automation + encryption on top of it.

### Full Auto / High Security (ransomware-proof)

Backups stored on Amazon AWS S3 with **Object Lock Compliance** enabled. It is Object Lock — a feature of AWS storage itself, not of this app — that makes the backups undeletable until their retention date. **The same mechanism banks, hospitals, and public administrations use for regulated archives.**

**Why this matters:** traditional backups on an external drive or a NAS get encrypted by ransomware alongside the source files — because they are reachable from the infected machine. Sophisticated ransomware also stays dormant for weeks before activating, so even backups taken just before the attack can already contain the infection. Object Lock solves both: the cloud storage itself refuses every delete and overwrite request for the lock window you picked, even from you, even from an attacker with your AWS password, even from AWS support.

**Guided 11-step wizard** that assumes zero AWS knowledge: it opens the AWS signup page for you, tells you exactly what to paste where, shows the estimated yearly cost for your data size before you commit, and creates the locked bucket for you. You do not touch the AWS console after that.

Best for: you want to be protected against the "my entire PC got ransomwared, including my backup drive" scenario.

---

## What happens after the wizard finishes

The app runs by itself from that point. Here is what it does for you in the background:

1. **Every day** — a short "differential" backup that uploads only the files changed since the last full. Fast, small, over in minutes.
2. **Once a month** — a full backup that uploads a complete copy. Runs while you are not using the computer (typically at night).
3. **Integrity checks** — each backup is verified by SHA-256 right after upload so you find out immediately if something went wrong, not six months later when you need to restore.
4. **Notifications** — optional email on success or failure, so you know the backup ran without having to open the app.
5. **Automatic cleanup** — old backups past their lock date are removed by S3 itself. You never pay for storage you do not need.

If the PC is off at the scheduled time, the app catches up on the next startup. If the internet is down, it retries. If a file is too big for your connection, it throttles. You do not manage any of this.

### Retention options

Chosen in the wizard, stored on the profile, applied by S3 server-side.

| Duration | Typical use |
|----------|-------------|
| **4 months** (default) | Covers the usual ransomware dwell time (around three months) with some margin. |
| **13 months** | Rolling one-year window with a month of overlap. |
| **7 years** | Common retention target for regulatory archives. |
| **Custom** | 2 to 20 years. |

### Cost transparency

The wizard shows an estimated total cost for each duration, against data sizes from 10 GB to 800 GB, based on Amazon AWS S3 Glacier Instant Retrieval list pricing. The number is indicative — actual invoices depend on your usage and the pricing AWS publishes at the time the bucket runs. Backup Manager does not bill and does not intermediate the AWS charges.

---

## Key features

| | Feature | Details |
|---|---------|---------|
| **S3 Object Lock** | High-security mode | Compliance mode — S3 refuses deletes and overwrites before the lock date |
| **4** | Storage backends | Local / USB, Network (UNC), SFTP (SSH), S3-compatible cloud |
| **+2** | Mirror copies | Independent replication with per-destination encryption |
| **AES-256** | Streaming encryption | GCM authenticated, no plaintext on disk (`.tar.wbenc`) |
| **GFS** | Retention rotation (classic mode) | Grandfather-Father-Son (daily / weekly / monthly) |
| **SHA-256** | Integrity verification | Pre-backup manifest + post-write check + periodic audits |
| **DPAPI** | Password storage | Windows user-bound, never in plaintext |
| **Adaptive** | Bandwidth management | Link speed probe + configurable throttling (25 / 50 / 75 / 100 %) |

---

## Quick start

### Install

Download **[BackupManager.msi](https://github.com/loicata/backup-manager/releases/latest)** and run it.

### First launch

The setup wizard guides you through creating the first profile.

**Classic** — 3 steps
1. Name the backup
2. Pick the folders to protect
3. Pick a destination (USB, network share, SFTP, or S3)

**Full Auto / High Security** — 11 steps
1. Read the protection summary
2. Pick a retention duration
3. Read the backup strategy (monthly full + daily differential)
4. Review the cost simulation
5. Read the disclaimers
6. Create or sign into an Amazon AWS account (guided)
7. Name the backup
8. Pick source folders
9. Optional encryption
10. Optional local mirror
11. Automatic bucket creation with Object Lock Compliance enabled

### From source

```bash
git clone https://github.com/loicata/backup-manager.git
cd backup-manager
pip install -r requirements.txt
python -m src
```

---

## Features in detail

### Multi-profile management

- Unlimited profiles, each with its own sources, destination, schedule, encryption, and retention.
- Mode selector in the General tab — profiles are filtered by mode.
- Active / inactive flag to pause a profile without losing its configuration.
- Reorder with Up / Down controls, switch in one click.
- Configuration validated before every run with readable error messages.

### Storage backends

| Destination | Description |
|-------------|-------------|
| Local / USB | Any local drive, external HDD, or removable USB. Auto-detection by hardware serial so a drive-letter change does not break the profile. |
| Network (UNC) | Windows shared folder (`\\server\share`) with username / password. Credentials go through Windows Credential Manager (`cmdkey`). |
| SFTP (SSH) | Password or private key (Ed25519, ECDSA, RSA). Server-side tar-stream upload / download (`tar cf -`) when the remote allows an exec channel. |
| Amazon AWS S3 | With optional Object Lock for the high-security mode. |
| S3-compatible | Scaleway, Wasabi, OVH, DigitalOcean, Cloudflare R2, Backblaze B2, MinIO. |

### Mirrors

- Up to **2 independent mirror copies** alongside the primary destination.
- Each mirror can use a **different storage type** and **its own encryption setting**.
- In the high-security mode, mirror failures are warnings — the primary S3 backup stays authoritative.
- GFS rotation runs independently on each destination.

### Backup types

| Type | Description |
|------|-------------|
| Full | Complete copy of the selected files. Self-contained restore point. |
| Differential | Only files changed since the last full. SHA-256 manifest comparison. Full-backup cycle configurable (default 30 days). |

### Retention

- **Classic mode** — Grandfather-Father-Son (daily / weekly / monthly counts, per destination).
- **High-security mode** — S3 Object Lock Compliance, S3 Lifecycle handles cleanup after the lock expires. The app never deletes locked objects.

### Scheduling and reliability

- Manual, hourly, daily, weekly, or monthly schedule.
- Optional auto-start at logon for unattended operation.
- Retry on failure with progressive delays (2, 10, 30, 90, 240 minutes).
- Pre-backup target check — all destinations verified before the run starts. Option to continue without a mirror if the primary storage is available.
- System tray mode for silent background operation.
- Missed-backup detection — runs automatically on next startup if a schedule was missed.
- Adaptive bandwidth probe — 16 MB sample for slow links (e.g. Starlink), 128 MB sample for fast ones. Throttling prevents network saturation.

### Recovery

- Local restore — browse a backup folder or pick an encrypted `.tar.wbenc` file.
- Remote restore — list + download from SFTP, S3, or a network share directly from the Recovery tab.
- Automatic decryption on the fly.
- Long-path support — Windows 260-character limit handled transparently via `\\?\` prefix.

### Email notifications

- SMTP alerts on success or failure.
- HTML report with file count, duration, destination, errors.
- Provider presets for Gmail, Outlook, Yahoo.

### Main interface

| Tab | Description |
|-----|-------------|
| Run | Launch a backup, watch real-time progress, bandwidth measurement, and logs. |
| General | Mode selector, profile name, backup type, source folders, exclusion patterns, bandwidth. |
| Storage | Primary destination type and connection settings. |
| Mirror 1 / 2 | Optional mirror destinations with their own storage and encryption. |
| Encryption | AES-256-GCM toggle per destination with password management. |
| Schedule | Frequency, time, auto-retry, periodic verification. |
| Protection | Object Lock status, retention duration, region, bucket (high-security mode). |
| Retention | GFS policy — daily / weekly / monthly counts (classic mode). |
| Email | SMTP settings with provider presets and test button. |
| Recovery | Restore from local or remote (SFTP, S3, network share). |
| Verify | On-demand integrity verification with real-time results. |
| History | Browse past backup logs, status column, right-click actions. |

---

## Security architecture

Defense in depth — independent layers, each designed to fail safely.

### S3 Object Lock (high-security mode)

| Layer | Mechanism |
|-------|-----------|
| Deletion resistance | S3 Object Lock Compliance — the bucket rejects delete and overwrite requests until the per-object retention date. |
| Full backups | Locked for the retention period + 30 days so the last full outlives its dependent differentials. |
| Differential backups | Locked for the retention period. |
| Cleanup | S3 Lifecycle removes objects after the lock expires. |
| No app-side delete | Backup Manager never issues a delete against a locked bucket. |

### Encryption at rest — `.tar.wbenc` streaming format

No plaintext data is ever written to disk.

```
.tar.wbenc file layout:

Header (37 bytes):
  [4B magic: "WBEC"]         — file format identifier
  [1B version: 0x01]         — format version
  [16B salt]                 — random salt for key derivation
  [16B reserved]             — future use (zeroed)

Body (repeating chunks):
  [4B plaintext_length]      — big-endian chunk size
  [12B nonce]                — sequential counter (never reused)
  [ciphertext + 16B GCM tag] — authenticated encrypted data

EOF sentinel:
  [4B zeros]                 — marks end of stream
```

### Cipher and key derivation

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Cipher | AES-256-GCM | NIST-approved authenticated encryption. |
| Key size | 256 bits | Largest AES key length. |
| Nonce | 12 bytes, sequential counter | Unique per chunk, never reused. |
| Authentication tag | 16 bytes (128 bits) | Detects tampering and corruption. |
| Key derivation | PBKDF2-HMAC-SHA256 | Standard password-based KDF. |
| Iterations | 600 000 | OWASP 2024 guidance. |
| Salt | 16 random bytes | `os.urandom()`, per-backup, prevents rainbow tables. |

### Password storage

| Platform | Method | Details |
|----------|--------|---------|
| Windows | DPAPI (`CryptProtectData`) | Tied to the current Windows user account. |
| Fallback | AES-256-GCM with a DPAPI-wrapped 32-byte machine key | Used if DPAPI is not available. |

### Summary

| Layer | Mechanism |
|-------|-----------|
| Ransomware resistance | S3 Object Lock Compliance (high-security mode). |
| Data at rest | AES-256-GCM streaming (`.tar.wbenc`). |
| Key derivation | PBKDF2-HMAC-SHA256, 600 000 iterations, random salt. |
| Password storage | Windows DPAPI + AES-256-GCM fallback. |
| Integrity | SHA-256 manifest + post-write verify + GCM auth tag. |
| Transport | SSH / HTTPS / SMB. |
| Memory | Explicit buffer zeroing on sensitive paths. |
| Path safety | Traversal-proof remote path validation. |
| Logging | No secrets in any log output. |
| Bug reports | Dual HMAC + Ed25519 signed diagnostics; injection filter on free-text. |
| Build | Nuitka native C compilation (no extractable bytecode). |

---

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=term-missing

# Run a specific file
pytest tests/unit/test_hashing.py -v
```

**Current status:** 1407 tests | 85 % coverage | 0 failures.

CI: GitHub Actions on every push — Black formatting, Ruff linting (Ubuntu), full test suite with coverage enforcement (Windows, Python 3.12 + 3.13).

---

## Build from source

### Prerequisites

- Python 3.11+ (tested on 3.12 and 3.13)
- [Nuitka](https://nuitka.net/) (Python → C compiler)
- MSVC Build Tools (C compiler for Nuitka)
- [WiX Toolset v3.14](https://wixtoolset.org/) (for MSI packaging only)

### Executable

```bash
python build_nuitka.py
```

Output: `dist/BackupManager/BackupManager.exe` (native C binary).

### MSI installer

```bash
python build_msi.py
```

Output: `dist/BackupManager-x.y.z.msi`.

---

## Project structure

```
backup-manager/
├── src/
│   ├── core/                        # Backup engine, scheduler, config, pipeline
│   │   ├── backup_engine.py         # Main orchestrator (11-phase pipeline)
│   │   ├── config.py                # Profile dataclasses + JSON persistence
│   │   ├── events.py                # Thread-safe event bus for UI updates
│   │   ├── bandwidth_tester.py      # Adaptive bandwidth measurement
│   │   ├── integrity_verifier.py    # Periodic backup integrity verification
│   │   ├── scheduler.py             # In-app scheduler + auto-start
│   │   └── phases/                  # Pipeline phases
│   │       ├── collector.py         # File collection + exclusion filtering
│   │       ├── filter.py            # Differential change detection
│   │       ├── encryptor.py         # Streaming tar encryption
│   │       ├── writer.py            # Write dispatcher (local / remote)
│   │       ├── verifier.py          # Post-write integrity verification
│   │       ├── mirror.py            # Mirror replication orchestrator
│   │       └── rotator.py           # GFS retention rotation
│   ├── storage/                     # Storage backends
│   │   ├── local.py                 # Local / USB with drive-serial detection
│   │   ├── network.py               # SMB / CIFS network shares
│   │   ├── sftp.py                  # SSH with tar-stream
│   │   ├── s3.py                    # S3 + Object Lock
│   │   ├── s3_setup.py              # Bucket provisioning + cost simulation
│   │   └── base.py                  # Abstract backend + retry + throttling
│   ├── security/                    # Encryption, DPAPI, secure memory
│   ├── notifications/               # SMTP email with HTML reports
│   └── ui/                          # Tkinter GUI (Sun Valley theme)
│       ├── wizard.py                # Classic (3 steps) + Pro (11 steps) wizard
│       ├── app.py                   # Main window with mode selector
│       └── tabs/                    # Tab implementations
│           ├── protection_tab.py    # Object Lock status (high-security mode)
│           └── ...                  # General, Storage, Mirror, etc.
├── tests/                           # 1407 tests (unit + integration)
├── CHANGELOG.md                     # Release history
├── requirements.txt                 # Runtime dependencies
└── pyproject.toml                   # Project metadata + tool config
```

---

## Requirements

| Requirement | Version |
|-------------|---------|
| OS | Windows 10 / 11 |
| Python | 3.11+ (dev only — end users install the MSI) |
| cryptography | >= 43.0.0 |
| paramiko | >= 3.0.0 |
| boto3 | >= 1.35.0 |
| Pillow | >= 10.0.0 |
| pystray | >= 0.19.0 |
| sv_ttk | >= 2.6.0 |

---

## License

[GNU General Public License v3.0](LICENSE) — Copyright (c) 2026 Loic Ader — [loicata.com](https://loicata.com)

---

## Contributing

Contributions are welcome. Please open an issue before submitting a pull request for any significant change.
