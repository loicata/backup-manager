@echo off
setlocal enabledelayedexpansion

echo.
echo  ================================================
echo   Backup Manager - EXE Builder
echo   Double-click and wait. That is all.
echo  ================================================
echo.

cd /d "%~dp0"

if not exist "gui.py" (
    echo  [ERROR] gui.py not found in %CD%
    echo  This script must be in the same folder as gui.py.
    pause
    exit /b 1
)

python --version >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python is not installed or not in PATH.
    echo  Download from: https://www.python.org/downloads/
    pause
    exit /b 1
)

:: Kill any running BackupManager.exe to release locked files
echo  [0/3] Closing running BackupManager instances...
taskkill /F /IM BackupManager.exe >nul 2>&1
if %errorlevel%==0 (
    echo        Closed running instance. Waiting 3 seconds...
    timeout /t 3 /nobreak >nul
) else (
    echo        No running instance found.
)

echo  [1/3] Installing PyInstaller...
pip install pyinstaller --quiet --disable-pip-version-check 2>nul
if errorlevel 1 pip install pyinstaller --user --quiet --disable-pip-version-check 2>nul
echo        Done.

echo  [2/3] Installing application dependencies...
pip install -r requirements.txt --quiet --disable-pip-version-check 2>nul
echo        Done.

echo  [3/3] Building BackupManager.exe (this may take 2-5 minutes)...
echo.

python build_pyinstaller.py

if errorlevel 1 (
    echo.
    echo  [ERROR] Build failed. See errors above.
    echo.
    echo  Common fixes:
    echo    - Run as Administrator
    echo    - Close BackupManager.exe if still running
    echo    - Disable antivirus temporarily
    echo    - Delete the dist\ and build\ folders manually, then retry
    echo    - Make sure Python is in PATH
    pause
    exit /b 1
)

echo.
echo  ================================================
echo   SUCCESS!
echo  ================================================
echo.
echo   Your EXE is in: %CD%\dist\BackupManager\
echo.
echo   How to use:
echo     1. Copy the entire dist\BackupManager folder
echo     2. Double-click BackupManager.exe to launch
echo     3. No Python needed on the target computer!
echo.
echo  ================================================
echo.

set /p LAUNCH="  Launch BackupManager.exe now? (Y/N): "
if /i "%LAUNCH%"=="Y" start "" "dist\BackupManager\BackupManager.exe"

pause
