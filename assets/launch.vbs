Set WshShell = CreateObject("WScript.Shell")
WshShell.Run Chr(34) & Replace(WScript.ScriptFullName, "launch.vbs", "BackupManager.exe") & Chr(34), 0, False
