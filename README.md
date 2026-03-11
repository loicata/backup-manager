# 💾 Backup Manager v2.2.9

Complete backup application for **Windows 10/11** built with **Python 3.13**.

---

## 🎯 Features

### Backup Types
- **Full backup**: complete copy of all selected files
- **Incremental backup**: only modified files since last backup (SHA-256 hash detection)
- **Differential backup**: only modified files since last full backup

### Storage Destinations
- 💿 **Local / external drive** (USB, SSD, HDD)
- 🌐 **Network folder** (UNC paths `\\server\share`, NAS, Samba)
- 🔒 **SFTP** (encrypted SSH transfer, password or key auth)
- ☁ **S3 / S3-compatible** (AWS, MinIO, Wasabi, OVH, Scaleway, DigitalOcean, Cloudflare R2...)
- ☁ **Azure Blob Storage**
- ☁ **Google Cloud Storage**
- 🔒 **Proton Drive** (via rclone, end-to-end encrypted)
- 🔄 **Mirror destinations** (3-2-1 rule with manifest upload, size verification, retry, and retention rotation)

### Bandwidth Throttling
- Per-profile upload speed limit (KB/s) to prevent network saturation
- Preset buttons: Unlimited, 10 Gbps, 1 Gbps, 100 Mbps (60% of link capacity)
- Applies to all backends: local copy, SFTP, S3, Azure, GCS, Proton Drive

### System Tray
- 🔔 **Notification area icon** with dynamic status (idle/running/success/error)
- Right-click menu: Show window, Run backup, Status, Quit
- **Windows toast notifications** on backup events
- Minimize to tray on close — app keeps running in the background

### Email Notifications
- 📧 **SMTP email reports** sent automatically after each backup
- 4 trigger modes: Disabled, On failure only, On success only, Always
- HTML formatted reports with backup summary
- Test email button to verify configuration

### Scheduling
- In-app scheduler (hourly, daily, weekly, monthly) with persistent state
- **Automatic retry on failure**: configurable attempts (1-8) with escalating delays
- Auto-start with Windows (Registry key)
- Missed backup detection after sleep/hibernation

### Encryption — 3 Modes
- 🔓 **No encryption** — all backups in plain text (fastest)
- 🔐 **Encrypt mirrors only** — primary stays plain, mirrors encrypted (best of both worlds)
- 🔒 **Encrypt everything** — primary + mirrors all encrypted (maximum security)
- **AES-256-GCM** with **PBKDF2-HMAC-SHA256** key derivation (600,000 iterations)

### Integrity & Retention
- **SHA-256 manifests** (.wbverify) for every backup
- **Application integrity check** at startup
- **Simple** or **GFS** retention on primary AND mirrors

### Drive Detection
- Waits for disconnected local drives before starting backup
- Polls every 2 seconds — auto-starts when drive is connected
- Tray notification + dialog with cancel button

### Interface
- 10 tabs: Run, General, Storage, Mirror, Encryption, Retention, Schedule, Email, History, Recovery
- Shield icon in title bar and taskbar
- Setup wizard (11 steps) — crash logging (crash.log)

---

## 📁 Project Structure

```
BackupManager/
├── gui.py               # Main GUI entry point (~3900 lines)
├── backup_engine.py     # Backup pipeline: collect → filter → copy → verify → encrypt → mirror → rotate
├── config.py            # Dataclass definitions, profile CRUD, DPAPI secrets
├── storage.py           # 7 storage backends + ThrottledReader bandwidth limiter
├── wizard.py            # 11-step setup wizard
├── scheduler.py         # In-app scheduler, journal, Windows auto-start
├── encryption.py        # AES-256-GCM encryption, DPAPI password storage
├── verification.py      # SHA-256 manifests, integrity verification
├── email_notifier.py    # SMTP email with HTML templates
├── tray.py              # System tray icon + toast notifications
├── installer.py         # Auto pip install of optional dependencies
├── integrity_check.py   # App self-verification (SHA-256)
├── secure_memory.py     # Best-effort password cleanup
├── build_pyinstaller.py # PyInstaller build with version metadata
├── setup_msi.py         # cx_Freeze MSI build (alternative)
├── innosetup.iss        # Inno Setup installer script
├── requirements.txt     # Python dependencies
├── BUILD_EXE.bat        # One-click build
└── README.md            # This file
```

---

## 🚀 Installation

```bash
cd C:\BackupManager
python gui.py
```

First launch: splash → auto-install deps → wizard → integrity check → app.

---

## 🔄 What's New in v2.2.9

- **3 encryption modes** — none / mirrors only / everything (in Encryption tab AND wizard)
- **Bandwidth throttling** — per-profile KB/s with presets (100 Mbps, 1 Gbps, 10 Gbps)
- **Mirror parity** — manifest upload, size verification, 3x retry, retention rotation
- **Drive detection** — waits for disconnected drives, auto-starts on connect
- **Email 4 radio presets** — disabled / failure / success / always
- **Shield icon** — replaces Tk feather in title bar and taskbar
- **Crash logging** — crash.log on startup failure
- **Startup splash** — visible window during init (no more invisible window)
- **All messageboxes** parent=self.root (no more mainloop death)
- **Post-backup protection** — try/except wrapper (app stays open)
- **Code documentation** — all 15 modules with architecture overviews

---

## 🏗 Building

See `BUILD_README.md`.

---

**Author**: Loïc Ader — loic@loicata.com | MIT License
