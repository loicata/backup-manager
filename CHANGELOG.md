# Changelog

All notable changes to Backup Manager are documented in this file.

Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[3.2.2]: https://github.com/loicata/backup-manager/releases/tag/v3.2.2
[3.1.2]: https://github.com/loicata/backup-manager/releases/tag/v3.1.2
[3.1.1]: https://github.com/loicata/backup-manager/releases/tag/v3.1.1
[3.0.1]: https://github.com/loicata/backup-manager/releases/tag/v3.0.1
