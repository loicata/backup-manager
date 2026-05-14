# End-to-end validation for a single backup run.
#
# Three phases, one command:
#   1. PRE-FLIGHT  -- environment + profile + network checks before launch
#   2. LIVE        -- tail log, highlight phases / PoC C signals / fallbacks
#   3. POST-RUN    -- Go/No-Go verdict with phase durations and deltas
#
# Generic across profile types -- adapts what it checks/shows to the
# storage backend declared in the profile JSON (local, sftp, s3,
# network). Falls back to safe defaults if the profile schema evolves.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts\validate-backup.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\validate-backup.ps1 `
#       -ProfileName "My Backup" -BaselineMinutes 131
#
# Workflow:
#   1. Run this command in a side terminal.
#   2. Pre-flight runs immediately and prints OK/FAIL per check.
#   3. If pre-flight is green, the script waits for the next backup
#      log line, then streams the live monitor.
#   4. When "Backup complete" or "Backup failed" fires, the post-run
#      verdict prints and the script exits.
#
# Exit codes: 0 = run validated OK, 1 = pre-flight failed,
#             2 = backup failed / cancelled, 3 = Ctrl+C interrupt.

[CmdletBinding()]
param(
    [string]$ProfileName = "",
    [double]$BaselineMinutes = 0
)

$ErrorActionPreference = "Stop"

$LogPath     = Join-Path $env:APPDATA "BackupManager\logs\backup_manager.log"
$ProfilesDir = Join-Path $env:APPDATA "BackupManager\profiles"
$RepoRoot    = Split-Path -Parent $PSScriptRoot
$VenvPython  = Join-Path $RepoRoot ".venv\Scripts\python.exe"

function Write-Check {
    param([bool]$ok, [string]$label, [string]$detail = "")
    $tag = if ($ok) { "[ OK ]" } else { "[FAIL]" }
    $color = if ($ok) { "Green" } else { "Red" }
    Write-Host ("{0,-7} {1}" -f $tag, $label) -ForegroundColor $color -NoNewline
    if ($detail) { Write-Host "  -- $detail" -ForegroundColor DarkGray } else { Write-Host "" }
    return $ok
}

# =====================================================================
# Phase 1: PRE-FLIGHT
# =====================================================================
Write-Host "===== PRE-FLIGHT =====" -ForegroundColor Cyan

$allChecks = @()

# Check 1: BM installed (via Add/Remove registry).
$bmEntry = Get-ItemProperty "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*", `
                            "HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*" `
    -ErrorAction SilentlyContinue | Where-Object { $_.DisplayName -eq "Backup Manager" } | Select-Object -First 1
$installedVersion = if ($bmEntry) { $bmEntry.DisplayVersion } else { "(not installed)" }
$allChecks += Write-Check ($null -ne $bmEntry) "Backup Manager installed" $installedVersion

# Check 2: BM not currently running (scheduler would race the test run).
$bmProc = Get-Process BackupManager -ErrorAction SilentlyContinue
$allChecks += Write-Check ($null -eq $bmProc) "BackupManager.exe not running" `
    $(if ($bmProc) { "PID $($bmProc.Id) -- close BM first" } else { "" })

# Check 3: venv python + deps importable (only matters if you want to
# rebuild from this terminal, but cheap to verify).
$venvOk = Test-Path $VenvPython
$allChecks += Write-Check $venvOk "venv python present" $VenvPython

# Check 4: log directory exists. Backups write here; if missing the
# install never ran or wrote anywhere unexpected.
$logDir = Split-Path -Parent $LogPath
$allChecks += Write-Check (Test-Path $logDir) "log directory exists" $logDir

# Check 5: profile resolution.
$profileJson = $null
$profileObj  = $null
if (Test-Path $ProfilesDir) {
    $profiles = Get-ChildItem $ProfilesDir -Filter "*.json"
    if ($ProfileName) {
        foreach ($p in $profiles) {
            $obj = Get-Content $p.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
            if ($obj.name -eq $ProfileName) { $profileJson = $p; $profileObj = $obj; break }
        }
    } elseif ($profiles.Count -eq 1) {
        # No name given but only one profile -- use it.
        $profileJson = $profiles[0]
        $profileObj  = Get-Content $profileJson.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
        $ProfileName = $profileObj.name
    }
}
$profOk = ($null -ne $profileObj)
$profDetail = if ($profOk) { "$ProfileName ($($profileObj.storage.storage_type))" } else { "no match" }
$allChecks += Write-Check $profOk "profile resolved" $profDetail

# Check 6: backend-specific reachability.
if ($profileObj) {
    switch ($profileObj.storage.storage_type) {
        "sftp" {
            $host_ = $profileObj.storage.sftp_host
            $port  = $profileObj.storage.sftp_port
            $tnc = Test-NetConnection -ComputerName $host_ -Port $port -WarningAction SilentlyContinue
            $allChecks += Write-Check $tnc.TcpTestSucceeded "SFTP host reachable" "${host_}:${port}"
            $keyPath = $profileObj.storage.sftp_key_path
            if ($keyPath) {
                $allChecks += Write-Check (Test-Path $keyPath) "SSH key file present" $keyPath
            }
        }
        "local" {
            $dest = $profileObj.storage.destination_path
            if ($dest) {
                $allChecks += Write-Check (Test-Path $dest) "local destination reachable" $dest
            }
        }
        "s3" {
            $bucket = $profileObj.storage.s3_bucket
            $allChecks += Write-Check ([bool]$bucket) "S3 bucket configured" $bucket
        }
        "network" {
            $dest = $profileObj.storage.destination_path
            $allChecks += Write-Check ([bool]$dest) "network destination set" $dest
        }
        default {
            $allChecks += Write-Check $true "backend $($profileObj.storage.storage_type) -- skip backend probe"
        }
    }
    # Source paths exist?
    foreach ($src in $profileObj.source_paths) {
        $allChecks += Write-Check (Test-Path $src) "source path reachable" $src
    }
}

# Check 7: previous run status -- warn if last run failed.
if ($profileObj -and $profileObj.last_backup_completed -eq $false) {
    Write-Check $true "previous run completed cleanly" "WARN: last run did not finish" | Out-Null
    Write-Host "         (incomplete_backup_name = '$($profileObj.incomplete_backup_name)')" -ForegroundColor Yellow
}

$failed = ($allChecks | Where-Object { -not $_ }).Count
if ($failed -gt 0) {
    Write-Host ""
    Write-Host "$failed pre-flight check(s) failed -- aborting." -ForegroundColor Red
    exit 1
}

# =====================================================================
# Phase 2: LIVE MONITOR
# =====================================================================
Write-Host ""
Write-Host "===== LIVE =====" -ForegroundColor Cyan
Write-Host "Pre-flight green. Launch the backup in BM now (or wait for the scheduler)."
Write-Host "Watching $LogPath ... (Ctrl+C to abort)"
Write-Host ""

# Track phase boundaries to time each phase.
$state = @{
    phaseStart = $null
    lastPhase  = $null
    runStart   = $null
    pocC       = @{ helperDeployed=$false; sidecarWritten=$false; sidecarUsed=$false }
    fallbacks  = @()
    errors     = @()
    phases     = [ordered]@{}
    verifySecs = $null
    totalMin   = $null
    status     = $null
}

function Parse-Ts([string]$line) {
    if ($line -match '^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})') {
        try { return [DateTime]::ParseExact($matches[1], "yyyy-MM-dd HH:mm:ss", $null) } catch {}
    }
    return $null
}

function Close-Phase([DateTime]$end) {
    if ($state.phaseStart -and $state.lastPhase) {
        $dur = ($end - $state.phaseStart).TotalSeconds
        $state.phases[$state.lastPhase] = $dur
        $mm = [int]([Math]::Floor($dur/60))
        $ss = [int]($dur - $mm*60)
        Write-Host ("    >>> {0} took {1}m {2}s" -f $state.lastPhase, $mm, $ss) -ForegroundColor DarkGray
        if ($state.lastPhase -eq "Verifying remote backup") {
            $state.verifySecs = $dur
        }
    }
}

$phaseRe = 'src\.core\.backup_engine: (Collecting files|Filtering changed files|Building integrity manifest|Uploading to Storage|Saving manifest|Verifying remote backup|Verifying backup|Writing commit marker|Uploading commit marker|Rotating old backups|Updating manifest)'

Get-Content -Path $LogPath -Wait -Tail 0 | ForEach-Object {
    $line = $_
    $ts = Parse-Ts $line

    if ($line -match $phaseRe) {
        $phase = $matches[1]
        Close-Phase $ts
        if (-not $state.runStart) { $state.runStart = $ts }
        $state.phaseStart = $ts
        $state.lastPhase = $phase
        Write-Host "[PHASE] $phase" -ForegroundColor Cyan
    }
    elseif ($line -match 'Deployed server helper to (/tmp/bm-helper-[^\s]+) \((\d+) bytes\)') {
        $state.pocC.helperDeployed = $true
        Write-Host "[OK]    Helper deployed: $($matches[1]) ($($matches[2]) B)" -ForegroundColor Green
    }
    elseif ($line -match 'Server helper sidecar written to ([^\s]+) \((\d+) bytes, (\d+) lines\)') {
        $state.pocC.sidecarWritten = $true
        $mb = [math]::Round([int64]$matches[2]/1MB, 1)
        Write-Host "[OK]    Sidecar written: $($matches[3]) lines, $mb MB" -ForegroundColor Green
    }
    elseif ($line -match 'Using server-helper sidecar \((\d+) hashes\) for') {
        $state.pocC.sidecarUsed = $true
        Write-Host "[OK]    Verify took sidecar path: $($matches[1]) hashes" -ForegroundColor Green
    }
    elseif ($line -match 'Remote verification OK: (\d+)/(\d+) files verified \((.+)\)') {
        Write-Host "[OK]    Remote verification: $($matches[1])/$($matches[2]) ($($matches[3]))" -ForegroundColor Green
    }
    elseif ($line -match 'Verification OK: (\d+)/(\d+)') {
        Write-Host "[OK]    Verification: $($matches[1])/$($matches[2])" -ForegroundColor Green
    }
    elseif ($line -match 'Server does not have GNU tar|Server helper (deployment failed|unavailable at runtime|emitted no hash|sidecar write to .* failed)|Sidecar .* (is empty|contained no usable lines)') {
        $state.fallbacks += $line.Substring([Math]::Max(0, $line.Length - 200))
        Write-Host "[WARN]  $line" -ForegroundColor Yellow
    }
    elseif ($line -match 'Helper stdout reader thread did not finish in time') {
        $state.errors += $line
        Write-Host "[FAIL]  Reader thread timeout (sftp.py:796)" -ForegroundColor Red
    }
    elseif ($line -match '\[ERROR\]|Backup failed:') {
        $state.errors += $line
        Write-Host "[ERR]   $line" -ForegroundColor Red
    }
    elseif ($line -match 'Backup complete: (\d+) files in ([\d.]+) min') {
        Close-Phase $ts
        $state.totalMin = [double]$matches[2]
        $state.status = "complete"
        $state.files = [int]$matches[1]
    }
    elseif ($line -match 'Backup (cancelled by user|failed|rejected)') {
        Close-Phase $ts
        $state.status = $matches[1]
    }

    if ($state.status) {
        # =================================================================
        # Phase 3: POST-RUN
        # =================================================================
        Write-Host ""
        Write-Host "===== POST-RUN =====" -ForegroundColor Cyan
        $statusColor = if ($state.status -eq "complete") { "Green" } else { "Red" }
        Write-Host ("Status: {0}" -f $state.status) -ForegroundColor $statusColor

        if ($state.totalMin) {
            $baseStr = if ($BaselineMinutes -gt 0) { "  (baseline {0} min, delta {1:+#.#;-#.#;0} min)" -f $BaselineMinutes, ($state.totalMin - $BaselineMinutes) } else { "" }
            Write-Host ("Total: {0:N1} min over {1} files{2}" -f $state.totalMin, $state.files, $baseStr)
        }

        if ($state.phases.Count -gt 0) {
            Write-Host ""
            Write-Host "Phase durations:"
            foreach ($p in $state.phases.Keys) {
                $sec = $state.phases[$p]
                $mm = [int]([Math]::Floor($sec/60))
                $ss = [int]($sec - $mm*60)
                Write-Host ("  {0,-32} {1,3}m {2,2}s" -f $p, $mm, $ss)
            }
        }

        if ($profileObj.storage.storage_type -eq "sftp") {
            Write-Host ""
            Write-Host "PoC C signals:"
            foreach ($k in @("helperDeployed","sidecarWritten","sidecarUsed")) {
                $color = if ($state.pocC[$k]) { "Green" } else { "Yellow" }
                Write-Host ("  {0,-18} {1}" -f $k, $state.pocC[$k]) -ForegroundColor $color
            }
            if ($state.verifySecs -ne $null) {
                $vColor = if ($state.verifySecs -lt 60) { "Green" } elseif ($state.verifySecs -lt 600) { "Yellow" } else { "Red" }
                Write-Host ("  verify duration   {0:N1} s" -f $state.verifySecs) -ForegroundColor $vColor
            }
        }

        Write-Host ""
        $verdict = "GO"
        if ($state.status -ne "complete") { $verdict = "NO-GO ($($state.status))" }
        elseif ($state.errors.Count -gt 0) { $verdict = "NO-GO ($($state.errors.Count) error(s))" }
        elseif ($state.fallbacks.Count -gt 0) { $verdict = "WARN ($($state.fallbacks.Count) fallback(s))" }
        elseif ($BaselineMinutes -gt 0 -and $state.totalMin -gt $BaselineMinutes) { $verdict = "WARN (slower than baseline)" }
        $vColor = if ($verdict -eq "GO") { "Green" } elseif ($verdict -match "WARN") { "Yellow" } else { "Red" }
        Write-Host "VERDICT: $verdict" -ForegroundColor $vColor

        if ($state.status -eq "complete") { exit 0 } else { exit 2 }
    }
}
