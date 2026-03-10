# Backup Manager v2.2.8 — Build Instructions

## Method 1: PyInstaller + Inno Setup (recommended)

### Step 1: Build with PyInstaller

```cmd
cd C:\BackupManager
python build_pyinstaller.py
```

Or double-click `BUILD_EXE.bat`.

Output: `build/dist/BackupManager/BackupManager.exe`

The build script:
- Kills any running BackupManager.exe
- Copies all 15 .py modules to build/src/
- Generates `version_info.txt` (Windows shows "Backup Manager" in taskbar, not "BackupManager.exe")
- Runs PyInstaller with --windowed and all hidden imports

### Step 2: Create installer with Inno Setup

1. Download Inno Setup from https://jrsoftware.org/isdl.php
2. Open `innosetup.iss` in Inno Setup Compiler
3. Click Build → Compile
4. Output: `build/dist/BackupManager_Setup_2.2.8.exe`

The installer includes:
- BackupManager.exe + all dependencies
- Desktop and Start Menu shortcuts
- Optional auto-start with Windows (Registry)
- Proper uninstaller (Add/Remove Programs)
- Launch after install checkbox

---

## Method 2: cx_Freeze MSI

```cmd
pip install cx_Freeze
python setup_msi.py bdist_msi
```

Output: `dist/BackupManager-2.2.8-win64.msi`

---

## What's in the build?

| File | Description |
|------|-------------|
| BackupManager.exe | Main application (windowed, no console) |
| _internal/ | Bundled Python 3.13 runtime + all packages |
| version_info.txt | Windows metadata: "Backup Manager" product name |

### 15 Python modules included:

| Module | Lines | Role |
|--------|-------|------|
| gui.py | ~3900 | Main window, 10 tabs, entry point |
| wizard.py | ~1950 | 11-step setup wizard |
| backup_engine.py | ~1400 | Backup pipeline (collect → mirror → rotate) |
| storage.py | ~1150 | 7 backends + bandwidth throttling |
| verification.py | ~850 | SHA-256 manifests |
| encryption.py | ~530 | AES-256-GCM + DPAPI |
| scheduler.py | ~490 | Scheduler + journal + auto-start |
| installer.py | ~470 | Auto pip install |
| config.py | ~430 | Profile management |
| tray.py | ~260 | System tray icon |
| email_notifier.py | ~260 | SMTP email reports |
| integrity_check.py | ~180 | App self-verification |
| secure_memory.py | ~130 | Password cleanup |
| build_pyinstaller.py | ~180 | PyInstaller build script |
| setup_msi.py | ~145 | cx_Freeze MSI build |

---

## System Requirements

### For building:
- Windows 10/11
- Python 3.13+
- ~500 MB disk space
- Internet connection (pip downloads)

### For running the installer:
- Windows 10/11 (64-bit)
- ~150 MB disk space
- No Python required (bundled)

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Build fails "Access denied" | Close BackupManager.exe first |
| exe doesn't start | Run from cmd to see errors |
| Window invisible | Check crash.log next to the exe |
| Missing DLL | Run from the dist/BackupManager/ folder (not just the .exe) |
| Tray icon missing | Ensure pystray + Pillow are installed |
