# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_submodules

hiddenimports = ['paramiko', 'paramiko.transport', 'paramiko.sftp_client', 'paramiko.ed25519key', 'paramiko.ecdsakey', 'paramiko.rsakey', 'paramiko.hostkeys', 'paramiko.auth_handler', 'paramiko.channel', 'boto3', 'botocore', 'botocore.config', 'botocore.exceptions', 'pyotp', 'pystray', 'pystray._win32', 'PIL', 'PIL.Image', 'PIL.ImageDraw', 'PIL.ImageFont', 'cryptography', 'cryptography.hazmat', 'cryptography.hazmat.primitives', 'cryptography.hazmat.primitives.ciphers', 'cryptography.hazmat.primitives.ciphers.aead']
hiddenimports += collect_submodules('paramiko')
hiddenimports += collect_submodules('pystray')
hiddenimports += collect_submodules('botocore')


a = Analysis(
    ['F:\\Documents\\loicata\\BackupManager\\Backup Manager v3\\.claude\\worktrees\\hardcore-jones\\src\\__main__.py'],
    pathex=['F:\\Documents\\loicata\\BackupManager\\Backup Manager v3\\.claude\\worktrees\\hardcore-jones'],
    binaries=[],
    datas=[('F:\\Documents\\loicata\\BackupManager\\Backup Manager v3\\.claude\\worktrees\\hardcore-jones\\assets\\backup_manager.ico', 'assets'), ('F:\\Documents\\loicata\\BackupManager\\Backup Manager v3\\.claude\\worktrees\\hardcore-jones\\assets\\License.rtf', 'assets'), ('F:\\Documents\\loicata\\BackupManager\\Backup Manager v3\\.claude\\worktrees\\hardcore-jones\\assets\\launch.vbs', '.')],
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='BackupManager',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['F:\\Documents\\loicata\\BackupManager\\Backup Manager v3\\.claude\\worktrees\\hardcore-jones\\assets\\backup_manager.ico'],
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='BackupManager',
)
