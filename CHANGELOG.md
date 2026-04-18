# Changelog

All notable changes to Backup Manager are documented in this file.

Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.3.6] - 2026-04-18

### Fixed
- SFTP restore of encrypted `.tar.wbenc` archives — probe remote with `sftp.stat()` to distinguish file vs directory layouts (previously always attempted `listdir_attr` and failed on encrypted single-file backups)
- SFTP long-path extraction — iterate tar members one by one with the Windows `\\?\` prefix so paths longer than 260 characters can be restored
- NETWORK authentication — replace `net use \\server\path * /user:… + stdin pipe` (which intermittently timed out on auth retry) with a `cmdkey /add` → `net use` → `cmdkey /delete` sequence using the Windows Credential Manager
- NETWORK recovery path — unified with the SFTP/S3 list+select+restore flow, removing the `WindowsPath empty name` bug that occurred on UNC roots
- NETWORK listing performance — fast mode that skips the recursive walk of tens of thousands of files; directory sizes computed asynchronously in the background with a progressive `…` placeholder in the UI
- Bug report under Nuitka — packaged-build detection now also recognises Nuitka (`__compiled__`) in addition to PyInstaller (`sys.frozen`); previously Nuitka binaries reported misleading `frozen: False`, tried to run `git rev-parse`, and produced `read_error` for every source hash
- Bug report — log/crash reading tolerates mixed UTF-8 / CP1252 encoding via `errors="replace"`; a single bad byte used to crash the whole report
- Bug report — `_send_report` now shows a clear error message instead of leaving the UI stuck when report generation fails
- Race condition on backup type — `_phase_update_delta` now uses a `forced_full` sentinel; `_save_profile` is blocked while a backup is running; profile switching skips the cascade when the same profile is re-selected
- USB wake-up extension — retry sequence `(0.3, 0.5, 1.0, 2.0, 4.0, 8.0)` up to ~16 s before failing, with silent auto-retry once in the pre-backup check
- Listbox selection loss on tab change — `exportselection=0` + fallback to `_current_profile` when no item is selected
- SFTP tar-stream download success was not logged in enough detail for users to know which code path ran

### Added
- Uploading phase marker (`Uploading encrypted archive: …`) in mirror uploads so users see progress through the boto3/SFTP upload stage
- Braille spinner at 10 FPS for Recovery scan/download progress (the previous three-dot animation at 500 ms was too subtle)
- Run tab header shows `auto-promoted` when a differential backup was silently promoted to a full backup because the profile configuration changed
- Live `BACKUP_TYPE_DETERMINED` event so the Run tab updates the header as soon as the engine decides the final backup type
- Verify tab incremental progress bar (cap 99 % during scan)
- History tab — Status column (parsed from log: success / cancelled / failed), double-click to open the log, right-click menu (Open / Copy path / Delete)
- Recovery Browse initial directory — 3-tier priority: existing path parent → profile storage root → OS default
- Schedule time auto-format — typing `2346` auto-inserts the colon to produce `23:46`
- Save button — pleine largeur, popup shown instantly before AutoStart + profile reload (previously appeared after ~200 ms)
- `scroll_to_widget()` on `ScrollableTab` — Recovery list auto-scrolls into view after `List available backups`
- Async NETWORK directory size compute — rglob runs in background after listing, UI updates progressively
- Regression tests for packaged-build detection, Unicode-tolerant log reading, race condition on backup type, Recovery Browse initial directory, Run tab header auto-promote, SFTP encrypted download, History tab status parsing (1403 tests total, +106 since 3.3.5)

### Changed
- Retention options reduced from five (`1 month / 4 months / 13 months / 7 years / 13 years`) to three (`4 months / 13 months / 7 years`) plus custom; default is now `4 months`
- Pro-mode wizard step 1 reworded to drop jargon (INDESTRUCTIBLE, IMMUTABLE, "guarantee") and explain that Object Lock is the component that protects the data
- Retention cost simulation table axes inverted — sizes are now rows and durations are columns, so extra sizes (400 GB, 800 GB) can be added vertically without widening the table

### Security
- Nothing publicly disclosed — see internal notes.

## [3.2.3] - 2026-04-08

### Fixed
- GFS rotation now filters by profile name prefix — backups from other profiles sharing the same storage are no longer counted or protected
- Rotation log "kept N" count no longer inflated by phantom .tar.wbenc entries

### Added
- Profile deletion now offers to delete all associated backups across all destinations (primary + mirrors)
- `sanitize_profile_name()` utility extracted for reuse
- `create_backend()` public factory function for storage backends
- `delete_profile_backups()` function for bulk cleanup across destinations

## [3.2.2] - 2026-04-03

### Added
- On-demand integrity verification from the Verify tab
- Scheduled periodic verification (configurable interval, default 7 days)
- Real-time verification results display
- Email verification reports with structured HTML table
- Clickable loicata.com link in sidebar and About dialog
- Schedule journal auto-refresh on profile load

### Changed
- Wider window (1520px) — all 12 tabs fully visible
- Real-time log display in Run tab (cross-thread Tkinter fix)
- Light gray input field borders (sv_ttk override)
- Source paths treeview height reduced for better layout
- Backup email: mirror destinations show description (SSH host, S3 bucket)
- Backup email: retention section shows backups available count

### Fixed
- GFS rotation: keep ALL backups within daily window (not just 1 per day)
- Silent return when no profile selected for verification
- `backup_manager.log` appearing as "Unknown" profile in History
- Flaky scheduler test (time-dependent)

### Removed
- Transfer rate and disk space remaining from email report
- Redundant log panel from Verify tab
- Outer scrollbar from History, Retention, Schedule, Encryption tabs

## [3.1.2] - 2026-03-27

### Fixed
- Duplicate profile names: prevent saving with identical names (case-insensitive)
- Auto-start at login: checkbox now correctly creates/removes VBS startup script
- Build scripts: `get_version()` correctly parses version from `src/__init__.py`

### Added
- Tests for duplicate profile name detection
- Tests for AutoStart VBS management (11 tests)

## [3.1.1] - 2026-03-21

### Added
- Recovery tab: simplified restore with Select backup + Restore destination
- Retrieve feature: download backups from remote servers (SFTP, S3, Proton Drive)
- Active / Inactive profiles with visual separation
- Reorder profiles with Up/Down buttons
- Retention UX: user-friendly values with dynamic summary

### Changed
- Minimum encryption password length increased to 16 characters
- Schedule defaults to Daily
- Window size optimized to 1400x900
- Proton Drive setup guide available on all mirrors

### Fixed
- Lambda late-binding bugs in exception handlers
- Missing import in storage tab
- History tab shows profile names instead of file IDs
- Auto-launch after MSI installation

## [3.0.1] - 2026-03-19

### Added
- Complete rewrite from v2.x with modular pipeline architecture
- Multi-backend storage: Local, USB, Network (UNC), SFTP, S3, Proton Drive
- Mirror destinations: up to 2 additional copies with independent encryption
- GFS retention: Grandfather-Father-Son rotation (daily/weekly/monthly)
- AES-256-GCM encryption with DPAPI-protected password storage
- SHA-256 integrity verification with post-backup checks
- Scheduled backups via Windows Task Scheduler with progressive retry
- Setup wizard for guided first-launch configuration
- System tray with background operation and single-instance support
- PBKDF2-HMAC-SHA256 key derivation (600,000 iterations)
- SFTP path traversal protection
- 492 tests, 90% coverage

[3.2.3]: https://github.com/loicata/backup-manager/releases/tag/v3.2.3
[3.2.2]: https://github.com/loicata/backup-manager/releases/tag/v3.2.2
[3.1.2]: https://github.com/loicata/backup-manager/releases/tag/v3.1.2
[3.1.1]: https://github.com/loicata/backup-manager/releases/tag/v3.1.1
[3.0.1]: https://github.com/loicata/backup-manager/releases/tag/v3.0.1
