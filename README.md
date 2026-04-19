# Backup Manager v3

[![CI](https://github.com/loicata/backup-manager/actions/workflows/ci.yml/badge.svg)](https://github.com/loicata/backup-manager/actions/workflows/ci.yml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/badge/tests-1429%20passed-brightgreen.svg)](#testing)
[![Coverage](https://img.shields.io/badge/coverage-85%25-brightgreen.svg)](#testing)
[![Platform](https://img.shields.io/badge/platform-Windows%2010%2F11-0078D6.svg)](https://github.com/loicata/backup-manager/releases)

## 🛡️ Backup Manager

**📦 Classic profiles** — backup to external drive, network share, SSH server, or S3 cloud storage.
**🔒 Anti-Ransomware profiles** — backup to Amazon AWS S3 with Object Lock, **the technology used in banking**. Your data becomes **impossible to delete**, even by ransomware.

✅ **100 % automatic** — daily backups, email alerts, integrity checks
✅ **No AWS knowledge needed** — the wizard creates and locks everything
✅ **Free and open-source** — no subscription, no account
✅ **Windows 10 / 11** — one-click MSI installer

### ⬇️ **[Download for Windows 10 / 11](https://github.com/loicata/backup-manager/releases/latest)**

| | |
|:---:|:---:|
| <img src="assets/screenshots/wizard_mode_choice.png" width="450"> | <img src="assets/screenshots/run_backup.png" width="450"> |
| <img src="assets/screenshots/general_tab.png" width="450"> | <img src="assets/screenshots/mirror_tab.png" width="450"> |

---

## Two modes

| | **Classic** | **Full Auto** (anti-ransomware) |
|---|---|---|
| Destination | USB / network share / SFTP / S3 | Amazon AWS S3 with Object Lock |
| Setup | 3 steps | 11 guided steps — AWS signup and bucket done for you |
| Protection | Encryption + integrity checks | Classic + backups that **cannot be deleted** before their retention date |
| For whom | You already have a drive or NAS | You want to survive a full ransomware attack |

## What Full Auto does once the wizard finishes

- 📅 Monthly full + daily differential (only changed files)
- 🔒 Each backup **locked on S3** for your chosen duration — even you cannot delete it
- ✅ SHA-256 integrity check after every upload
- 📧 Optional email on success / failure
- 🧹 Old backups past their lock date auto-deleted by S3 Lifecycle
- 🔁 Missed a run? Catches up on next startup. Retries on failure.

## Retention

| Duration | Use |
|---|---|
| **4 months** (default) | Ransomware dwell time is ~3 months — this covers it with margin. |
| **13 months** | Rolling one-year protection. |
| **7 years** | Regulatory archives. |
| **Custom** | 2 to 20 years. |

Cost shown in the wizard before you commit (10 GB → 800 GB, based on AWS S3 Glacier IR pricing). AWS bills you directly.

## Key features

| | |
|---|---|
| **S3 Object Lock Compliance** | Ransomware-proof, Full Auto mode |
| **4 storage backends** | Local / USB, network share, SFTP, S3 |
| **+2 mirror copies** | Independent, per-destination encryption |
| **AES-256-GCM streaming** | Nothing written in plaintext |
| **GFS rotation** (classic mode) | Daily / weekly / monthly |
| **SHA-256 integrity** | Pre, post, and periodic checks |
| **DPAPI password storage** | Windows user-bound |
| **Adaptive bandwidth** | Throttling for slow links (Starlink-tested) |

## Storage backends

| Destination | Description |
|---|---|
| **Local / USB** | Any local drive, external HDD, or removable USB. Auto-detection by hardware serial so drive-letter changes do not break the profile. |
| **Network (UNC)** | Windows shared folder (`\\server\share`) with username / password. Credentials go through Windows Credential Manager. |
| **SFTP (SSH)** | Password or private key (Ed25519, ECDSA, RSA). Server-side tar-stream when the remote allows an exec channel. |
| **Amazon AWS S3** | With optional Object Lock for the Full Auto mode. |
| **S3-compatible** | Scaleway, Wasabi, OVH, DigitalOcean, Cloudflare R2, Backblaze B2, MinIO. |

## Main interface

| Tab | Description |
|---|---|
| **Run** | Launch a backup, watch progress and logs |
| **General** | Profile name, type badge (Classic / Anti-Ransomware), source folders, exclusions, bandwidth |
| **Storage / Mirror 1 / Mirror 2** | Primary and up to 2 mirror destinations |
| **Encryption** | AES-256-GCM toggle per destination |
| **Schedule** | Frequency, time, auto-retry, periodic verification |
| **Protection** | Object Lock status, retention, bucket (Full Auto) |
| **Retention** | GFS policy (classic mode) |
| **Email** | SMTP with provider presets and test button |
| **Recovery** | Restore from local or remote (SFTP, S3, network) |
| **Verify** | On-demand integrity verification |
| **History** | Past backup logs with status column |

---

## Security architecture

Defense in depth — independent layers, each designed to fail safely.

### S3 Object Lock (Full Auto mode)

| Layer | Mechanism |
|---|---|
| **Deletion resistance** | S3 Object Lock Compliance — the bucket rejects delete and overwrite requests until the per-object retention date |
| **Full backups** | Locked for retention + 30 days so the last full outlives its dependent differentials |
| **Differential backups** | Locked for the retention period |
| **Cleanup** | S3 Lifecycle removes objects after the lock expires |
| **No app-side delete** | Backup Manager never issues a delete against a locked bucket |

### Encryption at rest — `.tar.wbenc` streaming format

No plaintext data is ever written to disk:

```
Header (37 B):
  [4B magic "WBEC"] [1B version] [16B salt] [16B reserved]

Body (repeating chunks):
  [4B plaintext length] [12B nonce] [ciphertext + 16B GCM tag]

EOF sentinel:
  [4B zeros]
```

### Cipher and key derivation

| Parameter | Value | Rationale |
|---|---|---|
| Cipher | AES-256-GCM | NIST-approved authenticated encryption |
| Key size | 256 bits | Largest AES key length |
| Nonce | 12 B sequential counter | Unique per chunk, never reused |
| Auth tag | 16 B (128 bit) | Detects tampering and corruption |
| KDF | PBKDF2-HMAC-SHA256 | Standard password-based KDF |
| Iterations | 600 000 | OWASP 2024 guidance |
| Salt | 16 B `os.urandom()` | Per-backup, prevents rainbow tables |

### Password storage

| Platform | Method | Details |
|---|---|---|
| Windows | DPAPI (`CryptProtectData`) | Tied to the current Windows user account |
| Fallback | AES-256-GCM with a DPAPI-wrapped 32-byte machine key | Used if DPAPI is unavailable |

### Summary

| Layer | Mechanism |
|---|---|
| Ransomware resistance | S3 Object Lock Compliance (Full Auto mode) |
| Data at rest | AES-256-GCM streaming (`.tar.wbenc`) |
| Key derivation | PBKDF2-HMAC-SHA256, 600 000 iterations, random salt |
| Password storage | Windows DPAPI + AES-256-GCM fallback |
| Integrity | SHA-256 manifest + post-write verify + GCM auth tag |
| Transport | SSH / HTTPS / SMB |
| Memory | Explicit buffer zeroing on sensitive paths |
| Path safety | Traversal-proof remote path validation |
| Logging | No secrets in any log output |
| Bug reports | Dual HMAC + Ed25519 signed diagnostics, injection-proof |
| Build | Nuitka native C compilation (no extractable bytecode) |

---

## Testing

```bash
pytest                                      # full suite
pytest --cov=src --cov-report=term-missing  # with coverage
```

**Current status:** 1429 tests, 85 % coverage, 0 failures.

CI (GitHub Actions, every push): Black formatting, Ruff linting (Ubuntu), full pytest with coverage enforcement (Windows, Python 3.12 + 3.13).

## Build from source

### Prerequisites
- Python 3.11+ (tested on 3.12 and 3.13)
- [Nuitka](https://nuitka.net/) (Python → C compiler)
- MSVC Build Tools
- [WiX Toolset v3.14](https://wixtoolset.org/) (MSI only)

### Commands
```bash
git clone https://github.com/loicata/backup-manager.git
cd backup-manager
pip install -r requirements.txt
python -m src                 # dev run
python build_nuitka.py        # -> dist/BackupManager/BackupManager.exe
python build_msi.py           # -> dist/BackupManager-x.y.z.msi
```

## Project structure

```
backup-manager/
├── src/
│   ├── core/                    # Backup engine, scheduler, config, pipeline
│   │   ├── backup_engine.py     # Main orchestrator (11-phase pipeline)
│   │   ├── config.py            # Profile dataclasses + JSON persistence
│   │   ├── events.py            # Thread-safe event bus for UI updates
│   │   ├── bandwidth_tester.py  # Adaptive bandwidth measurement
│   │   ├── integrity_verifier.py # Periodic integrity verification
│   │   ├── scheduler.py         # In-app scheduler + auto-start
│   │   └── phases/              # Pipeline phases
│   │       ├── collector.py     # File collection + exclusion filtering
│   │       ├── filter.py        # Differential change detection
│   │       ├── encryptor.py     # Streaming tar encryption
│   │       ├── writer.py        # Write dispatcher (local / remote)
│   │       ├── verifier.py      # Post-write integrity verification
│   │       ├── mirror.py        # Mirror replication orchestrator
│   │       └── rotator.py       # GFS retention rotation
│   ├── storage/                 # Storage backends
│   │   ├── local.py             # Local / USB with drive-serial detection
│   │   ├── network.py           # SMB / CIFS network shares
│   │   ├── sftp.py              # SSH with tar-stream
│   │   ├── s3.py                # S3 + Object Lock
│   │   ├── s3_setup.py          # Bucket provisioning + cost simulation
│   │   └── base.py              # Abstract backend + retry + throttling
│   ├── security/                # Encryption, DPAPI, secure memory
│   ├── notifications/           # SMTP email with HTML reports
│   └── ui/                      # Tkinter GUI (Sun Valley theme)
│       ├── wizard.py            # Classic (3 steps) + Pro (11 steps) wizard
│       ├── app.py               # Main window with sidebar (mode per profile)
│       └── tabs/                # Tab implementations
├── tests/                       # 1429 tests (unit + integration)
├── CHANGELOG.md
├── requirements.txt
└── pyproject.toml
```

## Requirements

| Requirement | Version |
|---|---|
| OS | Windows 10 / 11 |
| Python | 3.11+ (dev only — end users install the MSI) |
| cryptography | >= 43.0.0 |
| paramiko | >= 3.0.0 |
| boto3 | >= 1.35.0 |
| Pillow | >= 10.0.0 |
| pystray | >= 0.19.0 |
| sv_ttk | >= 2.6.0 |

---

**License** — [GPL v3.0](LICENSE) — © 2026 Loic Ader — [loicata.com](https://loicata.com)

**Issues / PRs** — welcome. Open an issue first for anything significant.
