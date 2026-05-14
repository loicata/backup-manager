# Real-time log monitor for PoC C Stage 5 validation.
#
# Tails $env:APPDATA\BackupManager\logs\backup_manager.log and
# highlights the messages that decide whether the helper path took
# effect or fell back to sequential verify. Tracks the duration of
# every phase so the verify-phase improvement is visible without
# manual timestamp arithmetic.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts\monitor-stage5.ps1
#   (run BEFORE launching the backup, leave it open in a side terminal)
#
# Exit: Ctrl+C
#
# Success criteria for Stage 5 (the three signals we want to see):
#   [OK]   Helper deployed to /tmp/bm-helper-XXXXXXXX.sh
#   [OK]   Sidecar written (~232k lines, ~25 MB)
#   [OK]   Verify took sidecar path (231908 hashes -- sequential skipped)
#
# At backup end the script prints a one-line summary of which signals
# fired, plus the duration of the verify phase compared to the legacy
# ~85 minute baseline.

$ErrorActionPreference = "Stop"

$LogPath = Join-Path $env:APPDATA "BackupManager\logs\backup_manager.log"
if (-not (Test-Path $LogPath)) {
    Write-Host "Log file not found: $LogPath" -ForegroundColor Red
    Write-Host "  (will appear when BackupManager.exe is launched)"
}

$phaseStart      = $null
$lastPhaseName   = $null
$verifyPhaseSecs = $null
$pocC = [ordered]@{
    helperDeployed = $false
    sidecarWritten = $false
    sidecarUsed    = $false
}

function Parse-Timestamp([string]$line) {
    if ($line -match '^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})') {
        try { return [DateTime]::ParseExact($matches[1], "yyyy-MM-dd HH:mm:ss", $null) }
        catch { return $null }
    }
    return $null
}

function Show-PhaseEnd($start, $end, $phase) {
    # Untyped on purpose: $start/$end may be $null on the very first
    # phase marker (no previous phase to close out yet). A strict
    # [DateTime] cast would reject the null before the body runs.
    if ($null -eq $start -or $null -eq $end -or [string]::IsNullOrEmpty($phase)) {
        return $null
    }
    $delta = ([DateTime]$end - [DateTime]$start).TotalSeconds
    $mm = [int]([Math]::Floor($delta / 60))
    $ss = [int]($delta - $mm * 60)
    Write-Host ("    --- {0} took {1}m {2}s" -f $phase, $mm, $ss) -ForegroundColor DarkGray
    return $delta
}

Write-Host "Monitoring $LogPath" -ForegroundColor Cyan
Write-Host "Watching PoC C signals... (Ctrl+C to stop)" -ForegroundColor Cyan
Write-Host ""

# Phase pattern matches what backup_engine._phase() writes via logger.info
$phaseRegex = 'src\.core\.backup_engine: (Collecting files|Filtering changed files|Building integrity manifest|Uploading to Storage|Saving manifest|Verifying backup \(hash\)|Writing commit marker|Uploading commit marker|Rotating old backups)'

Get-Content -Path $LogPath -Wait -Tail 0 | ForEach-Object {
    $line = $_
    $ts = Parse-Timestamp $line

    if ($line -match $phaseRegex) {
        $phase = $matches[1]
        $delta = Show-PhaseEnd $phaseStart $ts $lastPhaseName
        if ($lastPhaseName -eq 'Verifying backup (hash)' -and $delta -ne $null) {
            $script:verifyPhaseSecs = $delta
        }
        $phaseStart = $ts
        $lastPhaseName = $phase
        Write-Host "[PHASE] $phase" -ForegroundColor Cyan
    }
    elseif ($line -match 'Deployed server helper to (/tmp/bm-helper-[^\s]+) \((\d+) bytes\)') {
        $pocC.helperDeployed = $true
        Write-Host ("[OK]    Helper deployed to {0} ({1} bytes)" -f $matches[1], $matches[2]) -ForegroundColor Green
    }
    elseif ($line -match 'Server helper sidecar written to ([^\s]+) \((\d+) bytes, (\d+) lines\)') {
        $pocC.sidecarWritten = $true
        $mb = [math]::Round([int64]$matches[2] / 1MB, 1)
        Write-Host ("[OK]    Sidecar written: {0} lines, {1} MB" -f $matches[3], $mb) -ForegroundColor Green
    }
    elseif ($line -match 'Using server-helper sidecar \((\d+) hashes\) for ([^\s]+)') {
        $pocC.sidecarUsed = $true
        Write-Host ("[OK]    Verify took sidecar path: {0} hashes (sequential skipped)" -f $matches[1]) -ForegroundColor Green
    }
    elseif ($line -match 'Server does not have GNU tar') {
        Write-Host "[WARN]  FALLBACK: BSD tar on server, sequential verify will run" -ForegroundColor Yellow
    }
    elseif ($line -match 'Server helper deployment failed: (.+)$') {
        Write-Host ("[WARN]  FALLBACK: helper deploy failed -- {0}" -f $matches[1]) -ForegroundColor Yellow
    }
    elseif ($line -match 'Server helper unavailable at runtime \(([^)]+)\)') {
        Write-Host ("[WARN]  FALLBACK: helper runtime failure -- {0}" -f $matches[1]) -ForegroundColor Yellow
    }
    elseif ($line -match 'Server helper emitted no hash output') {
        Write-Host "[WARN]  Helper ran but emitted no hashes; sidecar will be absent" -ForegroundColor Yellow
    }
    elseif ($line -match 'Server helper sidecar write to ([^\s]+) failed: (.+) -- verify will fall back') {
        Write-Host ("[WARN]  Sidecar write failed (non-fatal) -- {0}" -f $matches[2]) -ForegroundColor Yellow
    }
    elseif ($line -match 'Sidecar ([^\s]+) (is empty|contained no usable lines)') {
        Write-Host ("[WARN]  Sidecar unusable: {0} -- {1}" -f $matches[1], $matches[2]) -ForegroundColor Yellow
    }
    elseif ($line -match 'Helper stdout reader thread did not finish in time') {
        Write-Host "[FAIL]  Reader thread timeout (sftp.py:796 -- known risk)" -ForegroundColor Red
    }
    elseif ($line -match '\[ERROR\]') {
        Write-Host $line -ForegroundColor Red
    }

    if ($line -match 'src\.core\.backup_engine: (Backup complete: (\d+) files in ([\d.]+) min|Backup failed: .+|Backup cancelled by user|Backup rejected: .+)') {
        $statusLine = $matches[1]
        Write-Host ""
        Write-Host ("===== {0} =====" -f $statusLine) -ForegroundColor Magenta
        Write-Host ("  Helper deployed : {0}" -f $pocC.helperDeployed) -ForegroundColor $(if ($pocC.helperDeployed) { "Green" } else { "Yellow" })
        Write-Host ("  Sidecar written : {0}" -f $pocC.sidecarWritten) -ForegroundColor $(if ($pocC.sidecarWritten) { "Green" } else { "Yellow" })
        Write-Host ("  Sidecar used    : {0}" -f $pocC.sidecarUsed) -ForegroundColor $(if ($pocC.sidecarUsed) { "Green" } else { "Yellow" })
        if ($verifyPhaseSecs) {
            $vm = [int]([Math]::Floor($verifyPhaseSecs / 60))
            $vs = [int]($verifyPhaseSecs - $vm * 60)
            Write-Host ("  Verify duration : {0}m {1}s  (baseline: ~85m)" -f $vm, $vs) -ForegroundColor $(if ($verifyPhaseSecs -lt 300) { "Green" } else { "Red" })
        }
        if ($matches[3]) {
            Write-Host ("  Total duration  : {0} min  (baseline: ~131m)" -f $matches[3]) -ForegroundColor $(if ([double]$matches[3] -lt 60) { "Green" } else { "Red" })
        }
        Write-Host ""
    }
}
