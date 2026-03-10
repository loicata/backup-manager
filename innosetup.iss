; ═══════════════════════════════════════════════════════════════
;  Backup Manager — Inno Setup Installer Script
;  Alternative to .msi — generates a professional .exe installer
;
;  Prerequisites:
;    1. Build with PyInstaller first: python build/build_pyinstaller.py
;    2. Then compile this .iss with Inno Setup: https://jrsoftware.org/isinfo.php
; ═══════════════════════════════════════════════════════════════

#define MyAppName "Backup Manager"
#define MyAppVersion "2.2.8"
#define MyAppPublisher "Loic Ader"
#define MyAppURL "mailto:loic@loicata.com"
#define MyAppExeName "BackupManager.exe"

[Setup]
AppId={{B4CKU9-M4N4-G3R1-0000-1NN0S3TUP}}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
DefaultDirName={autopf}\BackupManager
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
OutputDir=dist
OutputBaseFilename=BackupManager_Setup_{#MyAppVersion}
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
PrivilegesRequired=admin
SetupLogging=yes

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"
Name: "startupentry"; Description: "Start Backup Manager with Windows"; GroupDescription: "Startup:"

[Files]
; PyInstaller output folder
Source: "dist\BackupManager\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs
; Documentation
Source: "docs\*"; DestDir: "{app}\docs"; Flags: ignoreversion skipifsourcedoesntexist

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\Uninstall {#MyAppName}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Registry]
; Add to startup if selected
Root: HKCU; Subkey: "Software\Microsoft\Windows\CurrentVersion\Run"; \
  ValueType: string; ValueName: "BackupManager"; \
  ValueData: """{app}\{#MyAppExeName}"" --minimized"; \
  Flags: uninsdeletevalue; Tasks: startupentry

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Launch Backup Manager"; \
  Flags: nowait postinstall skipifsilent

[UninstallDelete]
Type: filesandordirs; Name: "{app}\__pycache__"
Type: filesandordirs; Name: "{app}\logs"
