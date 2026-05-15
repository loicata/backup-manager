<#
.SYNOPSIS
    Run a command under a virtual-memory cap. Kill if exceeded.

.DESCRIPTION
    Wraps a subprocess (and its direct children) under a periodic
    VM check. Defaults: 2048 MB cap, 500 ms poll. If the parent +
    children sum exceeds the cap, the whole tree is force-killed.

    Background:
        This repo had two PC freezes from python.exe accruing
        100+ GB virtual memory, saturating the pagefile, and
        locking the desktop hard (no BSOD). The cap trades one
        killed test for a saved session. Use it on every pytest /
        build / long-running Python invocation until the
        underlying leak is gone.

    Exit codes:
        0       command exited cleanly
        137     command was killed by the cap (mirrors POSIX SIGKILL)
        2       usage error
        other   propagated from the wrapped command

.PARAMETER LimitMB
    VM ceiling in megabytes for the parent + child tree. Default 2048.

.PARAMETER PollMs
    Sampling interval in milliseconds. Default 500. Lower = quicker
    reaction, higher CPU overhead.

.PARAMETER ChildrenRefreshTicks
    How often (in poll ticks) to refresh the list of child PIDs via
    CIM. Default 4 (i.e. every 2 s at PollMs=500). CIM queries are
    expensive; tick-throttling keeps the wrapper itself cheap.

.PARAMETER Exe
    The executable to run.

.PARAMETER ExeArgs
    Remaining positional arguments forwarded to the executable. Use
    quotes around items that contain spaces.

.EXAMPLE
    .\scripts\run-bounded.ps1 -LimitMB 2048 -Exe .\.venv\Scripts\python.exe -m pytest tests/test_sftp_tar_upload.py --no-cov

.EXAMPLE
    .\scripts\run-bounded.ps1 -Exe cmd.exe /c "echo hello"
#>
[CmdletBinding(PositionalBinding = $false)]
param(
    [int]$LimitMB = 2048,
    [int]$PollMs = 500,
    [int]$ChildrenRefreshTicks = 4,
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$Exe,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExeArgs
)

if ($null -eq $ExeArgs) { $ExeArgs = @() }
$argv = $ExeArgs

Write-Host "[run-bounded] exe=$Exe args=$argv"
Write-Host "[run-bounded] cap=${LimitMB}MB poll=${PollMs}ms"

# Use System.Diagnostics.Process directly. Start-Process -PassThru
# returns a wrapper object whose ExitCode is unreliable for fast-
# exiting children (often $null), which would silently mask test
# failures behind a green wrapper exit. .NET ProcessStartInfo gives
# us a real handle: WaitForExit() + ExitCode behave as expected.
#
# ArgumentList is .NET Core 6+ only; on Windows PowerShell 5.1 we
# must build a single Arguments string and quote any argument that
# contains whitespace.
$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = $Exe
$psi.UseShellExecute = $false
$psi.CreateNoWindow = $false
$quoted = foreach ($a in $argv) {
    if ($a -match '\s') { '"' + ($a -replace '"', '\"') + '"' } else { $a }
}
$psi.Arguments = ($quoted -join ' ')
$proc = [System.Diagnostics.Process]::Start($psi)
$parentId = $proc.Id
# Snapshot the start time so we can sanity-check children: a PID
# whose StartTime is older than our parent's cannot be ours (Windows
# recycles PIDs aggressively).
$parentStart = $proc.StartTime

$startTime = Get-Date
$peakMB = 0
$killed = $false
$reason = ''
$tickCount = 0
$childIds = @()

function Get-TreeBytes {
    # Sum committed private bytes (= what actually counts against the
    # pagefile, vs VirtualMemorySize64 which double-counts the huge
    # reserved-but-untouched address space allocators like jemalloc
    # or boto3's service catalog reserve up-front).
    param([int]$ParentId, [int[]]$ChildIds)

    $sum = 0
    $parent = Get-Process -Id $ParentId -ErrorAction SilentlyContinue
    if ($null -ne $parent) { $sum += $parent.PrivateMemorySize64 }
    foreach ($cid in $ChildIds) {
        $c = Get-Process -Id $cid -ErrorAction SilentlyContinue
        if ($null -ne $c) { $sum += $c.PrivateMemorySize64 }
    }
    return $sum
}

function Get-ChildPidList {
    # Recursive descendants of the wrapped parent (BFS via a single
    # CIM query). Critical for Python builders like Nuitka that spawn
    # cl.exe / link.exe as grandchildren — those can each commit
    # gigabytes; without recursion the wrapper would never see them
    # and the cap would silently leak.
    param([int]$ParentId, [datetime]$ParentStart)
    $allProcs = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue
    if ($null -eq $allProcs) { return @() }

    # Build parent_pid -> [child_pids] map in one pass
    $map = @{}
    foreach ($p in $allProcs) {
        $ppid = [int]$p.ParentProcessId
        if (-not $map.ContainsKey($ppid)) { $map[$ppid] = @() }
        $map[$ppid] += [int]$p.ProcessId
    }

    $result = @()
    $queue = [System.Collections.Queue]::new()
    $queue.Enqueue($ParentId)
    $seen = @{ $ParentId = $true }

    while ($queue.Count -gt 0) {
        $cur = $queue.Dequeue()
        if (-not $map.ContainsKey($cur)) { continue }
        foreach ($cid in $map[$cur]) {
            if ($seen.ContainsKey($cid)) { continue }
            # Validate that this PID started AFTER our parent: Windows
            # recycles PIDs aggressively and a stale recycled PID
            # would otherwise inflate the memory total.
            try {
                $cp = Get-Process -Id $cid -ErrorAction SilentlyContinue
                if ($null -ne $cp -and $cp.StartTime -ge $ParentStart) {
                    $result += $cid
                    $seen[$cid] = $true
                    $queue.Enqueue($cid)
                }
            } catch {}
        }
    }
    return $result
}

try {
    while (-not $proc.HasExited) {
        if (($tickCount % $ChildrenRefreshTicks) -eq 0) {
            $childIds = Get-ChildPidList -ParentId $parentId -ParentStart $parentStart
        }
        $tickCount++

        $totalBytes = Get-TreeBytes -ParentId $parentId -ChildIds $childIds
        $totalMB = [int]($totalBytes / 1MB)
        if ($totalMB -gt $peakMB) { $peakMB = $totalMB }

        if ($totalMB -gt $LimitMB) {
            $reason = "private commit ${totalMB}MB > cap ${LimitMB}MB"
            Write-Warning "[run-bounded] $reason -- killing tree"
            foreach ($treePid in (@($parentId) + $childIds)) {
                try { Stop-Process -Id $treePid -Force -ErrorAction SilentlyContinue } catch {}
            }
            $killed = $true
            break
        }

        Start-Sleep -Milliseconds $PollMs
    }
}
finally {
    # Even if HasExited became true mid-loop, WaitForExit() forces the
    # process object to refresh ExitCode (which is otherwise read once
    # at HasExited transition and may surface as $null in fast exits).
    try { $proc.WaitForExit(5000) | Out-Null } catch {}
    try { $proc.Refresh() } catch {}
}

$elapsed = [int]((Get-Date) - $startTime).TotalSeconds
if ($killed) {
    $exitCode = 137
    Write-Host "[run-bounded] KILLED  peak=${peakMB}MB elapsed=${elapsed}s reason='$reason'" -ForegroundColor Red
}
else {
    $exitCode = $proc.ExitCode
    $color = if ($exitCode -eq 0) { 'Green' } else { 'Yellow' }
    Write-Host "[run-bounded] exit=$exitCode peak=${peakMB}MB elapsed=${elapsed}s" -ForegroundColor $color
}
exit $exitCode
