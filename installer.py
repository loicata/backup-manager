"""
Backup Manager - Dependency Installer
=======================================
Automatic installation of Python packages at startup.

CRITICAL: check_module() uses importlib.find_spec() — NOT import_module().
This prevents side effects (e.g., ttkbootstrap monkey-patching tkinter).

Feature groups (each maps pip packages to a feature ID):
  FEAT_ENCRYPTION  → cryptography
  FEAT_S3          → boto3
  FEAT_AZURE       → azure-storage-blob
  FEAT_GCS         → google-cloud-storage
  FEAT_SFTP        → paramiko
  FEAT_TRAY        → pystray, Pillow, plyer

auto_install_all(parent):
  - Shows a Toplevel progress splash during installation
  - On failure, shows a detailed report of disabled features
  - parent parameter ensures Toplevel mode (no separate Tk root)

The UI grays out features whose dependencies are missing, but the app
still starts — only the affected features are disabled.
"""

import importlib
import importlib.util
import logging
import subprocess
import sys
from dataclasses import dataclass


def is_frozen() -> bool:
    """Check if running inside a PyInstaller bundle (.exe)."""
    return getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS')


# ──────────────────────────────────────────────
#  Feature IDs — referenced by GUI for enable/disable
# ──────────────────────────────────────────────
FEAT_ENCRYPTION   = "encryption"
FEAT_S3           = "storage_s3"
FEAT_AZURE        = "storage_azure"
FEAT_GCS          = "storage_gcs"
FEAT_SFTP         = "storage_sftp"
FEAT_TRAY         = "system_tray"


@dataclass
# ── Dependency descriptor ──
# Maps a pip package name to a feature ID and import module name.
# ALL_DEPENDENCIES lists every optional package the app can use.
class Dependency:
    """Represents a Python dependency."""
    pip_name: str           # Name for pip install
    import_name: str        # Name for import check (can differ from pip)
    description: str        # What it's used for
    feature_id: str         # Links to a feature constant above
    feature_label: str      # Human-readable feature name for UI messages
    required: bool = False  # True = core, False = optional feature


# ──────────────────────────────────────────────
#  Registry of all dependencies
# ──────────────────────────────────────────────
ALL_DEPENDENCIES = [
    # ── System Tray ──
    Dependency(
        "pystray", "pystray",
        "System tray icon (notification area)",
        FEAT_TRAY,
        "🔔 System tray icon & notifications",
    ),
    Dependency(
        "Pillow", "PIL",
        "Icon generation for system tray",
        FEAT_TRAY,
        "🔔 System tray icon & notifications",
    ),
    Dependency(
        "plyer", "plyer",
        "Native Windows toast notifications",
        FEAT_TRAY,
        "🔔 System tray icon & notifications",
    ),
    # ── Encryption ──
    Dependency(
        "cryptography", "cryptography",
        "AES-256-GCM Encryption",
        FEAT_ENCRYPTION,
        "🔐 Backup encryption (AES-256-GCM)",
    ),
    # ── Cloud Storage ──
    Dependency(
        "boto3", "boto3",
        "Amazon S3 / S3-compatible",
        FEAT_S3,
        "☁ S3 / S3-compatible storage (AWS, MinIO, Wasabi, OVH...)",
    ),
    Dependency(
        "azure-storage-blob", "azure.storage.blob",
        "Azure Blob Storage",
        FEAT_AZURE,
        "☁ Azure Blob Storage",
    ),
    Dependency(
        "google-cloud-storage", "google.cloud.storage",
        "Google Cloud Storage",
        FEAT_GCS,
        "☁ Google Cloud Storage",
    ),
    # ── SFTP ──
    Dependency(
        "paramiko", "paramiko",
        "SFTP transfers (SSH)",
        FEAT_SFTP,
        "🔒 SFTP transfers (SSH)",
    ),
]


def check_module(import_name: str) -> bool:
    """Check if a module is installed WITHOUT importing it.
    Uses importlib.util.find_spec which only checks the module exists
    on disk, without executing its code. This is critical for modules
    that monkey-patch other modules at import time."""
    try:
        spec = importlib.util.find_spec(import_name.split(".")[0])
        return spec is not None
    except (ModuleNotFoundError, ValueError):
        return False


# ── pip install wrapper ──
# Runs pip as subprocess to install a single package.
# Returns (success, output_message).
def install_package(pip_name: str) -> tuple[bool, str]:
    """
    Install a package using pip. Returns (success, message).
    Tries multiple strategies to handle Microsoft Store Python sandboxing
    and other common installation issues.
    """
    if is_frozen():
        return False, f"⚠ Cannot install modules in .exe mode: {pip_name}"

    # Strategy list: try each until one succeeds
    strategies = [
        # 1. Normal install
        [sys.executable, "-m", "pip", "install", pip_name,
         "--quiet", "--disable-pip-version-check"],
        # 2. With --user flag (Microsoft Store Python often needs this)
        [sys.executable, "-m", "pip", "install", pip_name,
         "--user", "--quiet", "--disable-pip-version-check"],
        # 3. With --no-build-isolation (helps with some C extension packages)
        [sys.executable, "-m", "pip", "install", pip_name,
         "--user", "--no-build-isolation", "--quiet", "--disable-pip-version-check"],
        # 4. Force reinstall (clears corrupted partial installs)
        [sys.executable, "-m", "pip", "install", pip_name,
         "--user", "--force-reinstall", "--quiet", "--disable-pip-version-check"],
    ]

    last_error = ""
    for i, cmd in enumerate(strategies):
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=180,
            )
            if result.returncode == 0:
                # Verify the module is actually importable now
                import_name = _get_import_name(pip_name)
                if import_name:
                    importlib.invalidate_caches()
                return True, f"✅ {pip_name} installed successfully"
            else:
                last_error = result.stderr.strip().split("\n")[-1] if result.stderr else "Unknown error"
        except subprocess.TimeoutExpired:
            last_error = "Installation timeout"
        except Exception as e:
            last_error = str(e)

    return False, f"❌ Failed to install {pip_name}: {last_error}"


def _get_import_name(pip_name: str) -> str:
    """Get the import name for a pip package name."""
    # ── Master list of all optional dependencies ──
    # grouped by feature: encryption, cloud storage, SFTP, system tray.
    for dep in ALL_DEPENDENCIES:
        if dep.pip_name == pip_name:
            return dep.import_name
    return ""


def check_all() -> tuple[list[Dependency], list[Dependency]]:
    """Check all dependencies. Returns (installed, missing)."""
    installed = []
    missing = []
    # ── Master list of all optional dependencies ──
    # grouped by feature: encryption, cloud storage, SFTP, system tray.
    for dep in ALL_DEPENDENCIES:
        if check_module(dep.import_name):
            installed.append(dep)
        else:
            missing.append(dep)
    return installed, missing


# ──────────────────────────────────────────────
#  Feature Availability (used by GUI)
# ──────────────────────────────────────────────
def get_available_features() -> dict[str, bool]:
    """
    Return a dict of feature_id -> is_available.
    A feature is available only if ALL its required modules are installed.
    """
    feature_modules: dict[str, list[bool]] = {}
    # ── Master list of all optional dependencies ──
    # grouped by feature: encryption, cloud storage, SFTP, system tray.
    for dep in ALL_DEPENDENCIES:
        if dep.feature_id not in feature_modules:
            feature_modules[dep.feature_id] = []
        feature_modules[dep.feature_id].append(check_module(dep.import_name))

    return {
        feat: all(checks)
        for feat, checks in feature_modules.items()
    }


def get_unavailable_features_detail() -> list[dict]:
    """
    Return a list of unavailable features with details.
    Each item: {"feature_id", "feature_label", "missing_modules": [pip_name, ...]}
    """
    features = get_available_features()
    result = []
    seen_features = set()

    for feat_id, available in features.items():
        if available or feat_id in seen_features:
            continue
        seen_features.add(feat_id)

        missing_mods = []
        label = ""
        # ── Master list of all optional dependencies ──
        # grouped by feature: encryption, cloud storage, SFTP, system tray.
        for dep in ALL_DEPENDENCIES:
            if dep.feature_id == feat_id:
                if not label:
                    label = dep.feature_label
                if not check_module(dep.import_name):
                    missing_mods.append(dep.pip_name)
        if missing_mods:
            result.append({
                "feature_id": feat_id,
                "feature_label": label,
                "missing_modules": missing_mods,
            })

    return result


def install_all_missing(callback=None) -> tuple[list[str], list[str]]:
    """Install all missing dependencies. Returns (successes, failures)."""
    _, missing = check_all()
    if not missing:
        if callback:
            callback("✅ All dependencies are already installed.")
        return [], []

    successes = []
    failures = []
    for i, dep in enumerate(missing):
        if callback:
            callback(f"[{i+1}/{len(missing)}] Installation de {dep.pip_name} ({dep.description})...")
        success, result_msg = install_package(dep.pip_name)
        if callback:
            callback(result_msg)
        if success:
            successes.append(dep.pip_name)
        else:
            failures.append(dep.pip_name)
    return successes, failures


def install_selected(pip_names: list[str], callback=None) -> tuple[list[str], list[str]]:
    """Install only selected packages. Returns (successes, failures)."""
    successes = []
    failures = []
    for i, name in enumerate(pip_names):
        if callback:
            callback(f"[{i+1}/{len(pip_names)}] Installation de {name}...")
        success, result_msg = install_package(name)
        if callback:
            callback(result_msg)
        if success:
            successes.append(name)
        else:
            failures.append(name)
    return successes, failures


# ──────────────────────────────────────────────
#  Startup Auto-Installer (silent + failure report)
# ──────────────────────────────────────────────
def auto_install_all(parent=None):
    """
    Automatically install all missing dependencies at startup.
    Shows a minimal splash with progress. On failure, shows a detailed
    report of disabled features and waits for user acknowledgment.

    Args:
        parent: Optional parent Tk window. If given, splash is a Toplevel
                (avoids Tcl interpreter destruction issues).

    Skipped entirely when running as a PyInstaller frozen executable,
    since all modules are already bundled in the .exe.
    """
    if is_frozen():
        return

    # Upgrade pip first (silently, helps with Microsoft Store Python)
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "--upgrade", "pip",
             "--quiet", "--disable-pip-version-check", "--user"],
            capture_output=True, text=True, timeout=60,
        )
    except Exception as e:
        logging.getLogger(__name__).debug(f"Pip upgrade skipped: {e}")

    _, missing = check_all()

    if not missing:
        return

    import tkinter as tk
    from tkinter import ttk

    if parent:
        splash = tk.Toplevel(parent)
    else:
        splash = tk.Tk()
    splash.title("Backup Manager — Preparing...")
    splash.geometry("500x180")
    splash.resizable(False, False)

    splash.update_idletasks()
    x = (splash.winfo_screenwidth() - 500) // 2
    y = (splash.winfo_screenheight() - 180) // 2
    splash.geometry(f"500x180+{x}+{y}")

    if parent:
        splash.transient(parent)
        splash.grab_set()

    ttk.Label(splash, text="📦 Backup Manager — Installing modules",
              font=("Segoe UI", 12, "bold")).pack(padx=20, pady=(15, 5), anchor="w")

    status_label = ttk.Label(splash, text="Analyzing dependencies...",
                              font=("Segoe UI", 9))
    status_label.pack(padx=20, anchor="w")

    detail_label = ttk.Label(splash, text="", font=("Segoe UI", 8),
                              foreground="#666")
    detail_label.pack(padx=20, anchor="w", pady=(2, 5))

    progress = ttk.Progressbar(splash, maximum=len(missing), length=460,
                                mode="determinate")
    progress.pack(padx=20, pady=(0, 15))

    splash.update()

    successes = 0
    failures = []

    for i, dep in enumerate(missing):
        status_label.configure(
            text=f"Installation de {dep.pip_name}... ({i+1}/{len(missing)})"
        )
        detail_label.configure(text=dep.description)
        progress["value"] = i
        splash.update()

        success, _ = install_package(dep.pip_name)
        if success:
            successes += 1
        else:
            failures.append(dep.pip_name)

    progress["value"] = len(missing)
    splash.update()
    try:
        splash.grab_release()
    except Exception:
        pass
    splash.destroy()

    if failures:
        _show_failure_report(failed_packages=failures, parent=parent)


# ── Post-install report for failed packages ──
# Shows which features are disabled and why.
# User must acknowledge before the app continues.
def _show_failure_report(failed_packages: list[str], parent=None):
    """
    Show a detailed window explaining which features are disabled
    due to failed module installations. Waits for user acknowledgment.
    """
    import tkinter as tk
    from tkinter import ttk

    unavailable = get_unavailable_features_detail()

    if parent:
        root = tk.Toplevel(parent)
    else:
        root = tk.Tk()
    root.title("Backup Manager — Missing modules")
    root.geometry("650x500")
    root.resizable(False, False)

    root.update_idletasks()
    x = (root.winfo_screenwidth() - 650) // 2
    y = (root.winfo_screenheight() - 500) // 2
    root.geometry(f"650x500+{x}+{y}")

    if parent:
        root.transient(parent)
        root.grab_set()

    # Header
    ttk.Label(root,
              text="⚠ Some modules could not be installed",
              font=("Segoe UI", 13, "bold")).pack(padx=20, pady=(15, 5), anchor="w")

    ttk.Label(root,
              text="Backup Manager will start, but the following features\n"
                   "will be disabled and grayed out in the interface :",
              font=("Segoe UI", 9)).pack(padx=20, anchor="w", pady=(0, 10))

    # Feature list
    list_frame = ttk.Frame(root)
    list_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 10))

    text = tk.Text(list_frame, font=("Segoe UI", 10), wrap=tk.WORD,
                    relief=tk.SOLID, bd=1, bg="#fff8f0",
                    state=tk.NORMAL, cursor="arrow")
    scroll = ttk.Scrollbar(list_frame, command=text.yview)
    text.configure(yscrollcommand=scroll.set)
    text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    scroll.pack(side=tk.RIGHT, fill=tk.Y)

    for info in unavailable:
        text.insert(tk.END, f"\n  ✕  {info['feature_label']}\n", "feature")
        modules_str = ", ".join(info["missing_modules"])
        text.insert(tk.END, f"       Module(s) missing : {modules_str}\n", "detail")

    if not unavailable and failed_packages:
        text.insert(tk.END, f"\n  Modules failed : {', '.join(failed_packages)}\n", "detail")

    text.tag_configure("feature", foreground="#c0392b", font=("Segoe UI", 10, "bold"))
    text.tag_configure("detail", foreground="#7f8c8d", font=("Consolas", 9))
    text.configure(state=tk.DISABLED)

    # Info about module manager
    info_frame = tk.Frame(root, bg="#d5f5e3", padx=15, pady=8)
    info_frame.pack(fill=tk.X, padx=20, pady=(0, 10))
    tk.Label(info_frame,
             text="💡 You can retry installation at any time\n"
                  "     via the button « 📦 Manage modules » in the sidebar\n"
                  "     of the application.",
             bg="#d5f5e3", fg="#1e8449", font=("Segoe UI", 9),
             justify=tk.LEFT).pack(anchor="w")

    # Acknowledgment button
    btn_frame = ttk.Frame(root)
    btn_frame.pack(fill=tk.X, padx=20, pady=(0, 15))

    def _dismiss():
        try:
            root.grab_release()
        except Exception:
            pass
        root.destroy()

    ttk.Button(
        btn_frame,
        text='✅ I understand — Launch Backup Manager',
        command=_dismiss,
    ).pack(side=tk.RIGHT)

    root.protocol("WM_DELETE_WINDOW", _dismiss)
    if parent:
        parent.wait_window(root)  # Block until dismissed (Toplevel mode)
    else:
        root.mainloop()  # Standalone mode
