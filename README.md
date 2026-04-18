# Backup Manager v3

[![CI](https://github.com/loicata/backup-manager/actions/workflows/ci.yml/badge.svg)](https://github.com/loicata/backup-manager/actions/workflows/ci.yml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/badge/tests-1407%20passed-brightgreen.svg)](#testing)
[![Coverage](https://img.shields.io/badge/coverage-85%25-brightgreen.svg)](#testing)
[![Platform](https://img.shields.io/badge/platform-Windows%2010%2F11-0078D6.svg)](https://github.com/loicata/backup-manager/releases)

## Backups that even ransomware cannot delete.

- 🛡️ **Ransomware-proof** — backups locked on Amazon S3 for **4 months to 7 years**. Undeletable. By anyone.
- 🖱️ **For non-technicians** — 3-step wizard, zero AWS knowledge, bucket created and locked for you.
- ⚡ **Complete out of the box** — AES-256 encryption, scheduling, 3 parallel destinations, integrity checks, email alerts.

### ➡️ **[Download for Windows 10 / 11](https://github.com/loicata/backup-manager/releases/latest)**

| | |
|:---:|:---:|
| <img src="assets/screenshots/wizard_mode_choice.png" width="450"> | <img src="assets/screenshots/run_backup.png" width="450"> |
| <img src="assets/screenshots/general_tab.png" width="450"> | <img src="assets/screenshots/mirror_tab.png" width="450"> |

---

## Two modes

| | Classic | Full Auto (anti-ransomware) |
|---|---|---|
| **Destination** | USB / network share / SFTP / S3 | Amazon AWS S3 with Object Lock |
| **Setup** | 3 steps | 11 guided steps — AWS signup & bucket done for you |
| **Protection** | Encryption + integrity checks | Classic + backups that **cannot be deleted** before their retention date |
| **For whom** | You already have a drive or NAS | You want to survive a full ransomware attack |

## What Full Auto does once the wizard finishes

- 📅 Monthly full + daily differential (only changed files).
- 🔒 Each backup **locked on S3** for your chosen duration — even you cannot delete it.
- ✅ SHA-256 integrity check after every upload.
- 📧 Optional email on success / failure.
- 🧹 Old backups past their lock date auto-deleted by S3 Lifecycle.
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

---

<details>
<summary><b>Security architecture (for developers)</b></summary>

### S3 Object Lock (Full Auto mode)
| Layer | Mechanism |
|---|---|
| Deletion resistance | S3 Object Lock Compliance — bucket rejects delete and overwrite until the object's retention date |
| Full backups | Locked for retention + 30 days (so they outlive dependent diffs) |
| Differential backups | Locked for retention period |
| Cleanup | S3 Lifecycle after lock expires |
| No app-side delete | App never issues a delete against a locked bucket |

### `.tar.wbenc` streaming format — no plaintext on disk
```
Header (37 B):  ["WBEC" 4B] [ver 1B] [salt 16B] [reserved 16B]
Body chunks:    [len 4B] [nonce 12B] [ciphertext + GCM tag 16B]
EOF:            [0x00000000]
```

### Cipher / KDF
| Parameter | Value |
|---|---|
| Cipher | AES-256-GCM |
| Nonce | 12 B sequential counter, never reused |
| Auth tag | 16 B (128 bit) |
| KDF | PBKDF2-HMAC-SHA256 |
| Iterations | 600 000 (OWASP 2024) |
| Salt | 16 B `os.urandom()` |

### Summary
| Layer | Mechanism |
|---|---|
| Ransomware | S3 Object Lock Compliance |
| Data at rest | AES-256-GCM streaming (`.tar.wbenc`) |
| Password | DPAPI + AES-256-GCM fallback |
| Integrity | SHA-256 manifest + GCM tag + post-write verify |
| Transport | SSH / HTTPS / SMB |
| Path safety | Traversal-proof remote path validation |
| Bug reports | Dual HMAC + Ed25519 signed, injection-proof |
| Build | Nuitka native C compilation (no extractable bytecode) |

</details>

<details>
<summary><b>Build from source</b></summary>

```bash
git clone https://github.com/loicata/backup-manager.git
cd backup-manager
pip install -r requirements.txt
python -m src                 # dev run
python build_nuitka.py        # -> dist/BackupManager/BackupManager.exe
python build_msi.py           # -> dist/BackupManager-x.y.z.msi
```

Prerequisites: Python 3.11+, Nuitka, MSVC Build Tools, [WiX Toolset v3.14](https://wixtoolset.org/) (MSI only).

</details>

<details>
<summary><b>Testing</b></summary>

```bash
pytest                                      # 1407 tests, 85 % coverage
pytest --cov=src --cov-report=term-missing
```

CI runs Black + Ruff + full pytest on Windows (Python 3.12 + 3.13).

</details>

---

**License** — [GPL v3.0](LICENSE) — © 2026 Loic Ader — [loicata.com](https://loicata.com)

**Issues / PRs** — welcome. Open an issue first for anything significant.
