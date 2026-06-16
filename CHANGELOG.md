# Changelog

All notable changes to Backup Manager are documented in this file.

Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.7.56] - 2026-06-16

Same-day follow-up to 3.7.55, which fixed only the manifest phase. The first runtime test of
3.7.55 surfaced an identical bug in **three other write paths**: the tar streamer killed a 2h13
SFTP backup after the manifest had skipped 1 951 unreadable files cleanly, because one of the
files the manifest accepted became unreadable later (bad-blocks-shifting) and the tar writer
had no equivalent tolerance.

### Fixed — reliability
- **The tar-stream writers now skip unreadable source files instead of aborting the upload.** The
  v3.7.55 manifest fix was applied symmetrically to the three remaining unguarded sites:
  ``remote_writer._build_encrypted_tar`` (encrypted SFTP/S3 tar), ``local_writer.write_encrypted_tar``
  (encrypted local tar), and ``SFTPStorage.upload_tar_stream`` (plain SFTP tar, both the classic and
  helper paths). A source file whose ``open()`` raises any ``OSError`` is now logged, recorded under
  ``skipped_files``, and dropped from the tar stream; the run continues. Files that vanish mid-read
  (after ``tar.addfile()`` has written the entry header) still abort — once the tar stream is in an
  entry, skipping would leave a corrupt archive — but that case is much rarer than ``open()``-time
  failures.
- ``_upload_tar_batch`` now receives the integrity manifest and prunes it against the skipped set
  the SFTP writer reports, so the ``.wbverify`` sidecar saved next phase matches what actually
  landed on the remote — verify can no longer flag the skipped files as "missing" forever.

## [3.7.55] - 2026-06-16

Fix for a backup crash-loop: a single unreadable source file (e.g. a corrupt forensic dump on a
failing disk) aborted the entire backup, which the scheduler then retried 5×/day and crash-recovery
relaunched on every restart — an infinite storm that also bloated the run history to hundreds of MB
and drove memory use into the gigabytes.

### Fixed — reliability
- **An unreadable source file no longer aborts the whole backup.** ``build_integrity_manifest``
  caught only ``FileNotFoundError`` (a vanished file); any other ``OSError`` — permission denied,
  ``[Errno 22]`` on a corrupt/forensic file, a share dropping mid-read — propagated and killed the
  run, which the scheduler then retried forever. Such files are now skipped and recorded under
  ``skipped_files`` (reason ``unreadable_before_hash``), surfaced as a per-file warning plus a count
  in the Run log — never hidden behind a recomputed checksum. This aligns the manifest with the
  filter phase, which already tolerated these errors.
- The write, verify and mirror phases no longer re-open those files: ``_phase_integrity`` drops the
  unreadable paths from the working set once the manifest has identified them. Vanished files keep
  their existing behaviour (the writers tolerate and prune them).

### Fixed — run history
- **A single oversized log event can no longer bloat the per-profile history file.** The collector's
  "Skipped N file(s)" event embeds every skipped path and could reach tens of MB; written once per
  retry/crash-recovery cycle it grew one profile's JSONL to 722 MB and fed a load-time memory spike.
  Each persisted line is now capped — oversized ``details`` lists are sampled with an omitted-count,
  or dropped entirely — so the file stays bounded regardless of payload size.

## [3.7.54] - 2026-06-11

Post-audit coherence review of the 3.7.49→3.7.53 fix marathon, cross-checked against the
first scheduled-run logs on the Nuitka build (all clean). One real finding fixed, plus the
remaining lint leftovers.

### Fixed — data protection
- **A failure or user Cancel during rotation could destroy committed mirror backups.** The
  committed-primary protection (3.7.49) did not extend to mirrors: ``_commit_mirror`` recorded
  nothing, so ``_best_effort_cleanup`` unconditionally deleted every mirror artefact — including
  ones whose ``.wbcommit`` had just been written, leaving an orphan marker behind and wiping a
  valid, verified mirror copy. Each committed mirror's index is now recorded in
  ``ctx.mirrors_committed`` and the cleanup keeps those artefacts, logging the keep exactly like
  the primary's. (No user profile currently configures a mirror, so no run was exposed.)
- The cleanup's mirror loop no longer skips silently when a mirror backend cannot be built —
  the skip and its reason are logged (the orphan scan reclaims any leftovers at the next run).

### Code quality
- Cleared every remaining ruff finding: unused ``PROGRESS`` import, unsorted imports and a
  manual index counter (→ ``enumerate``) in ``backup_engine.py``; 7 specific-exception
  ``try/except/pass`` blocks rewritten as ``contextlib.suppress`` in ``_fs_utils``,
  ``progress_panel`` and ``wizard``. ``ruff check src/`` is now fully clean.

## [3.7.53] - 2026-06-10

Closes the 2026-06-10 deep audit entirely: the 25 actionable low-severity findings are fixed
(1 was already fixed in 3.7.52, 1 was refuted on re-verification). The audit backlog is now
empty across all severities. Full suite: 2775 passed, 28 skipped.

### Fixed — verification & cancellation
- **A backup whose ``.wbverify`` sidecar failed to write was committed "verified" with zero integrity reference.** ``verify_backup`` now accepts the engine's in-memory manifest and runs the FULL check against it (missing files, parallel re-hash, extras, total-checksum guard) when the sidecar is absent.
- **Cancelling a periodic verification did nothing** for the duration of a backup's full re-hash: the cancel hook returned a bool instead of raising. ``IntegrityVerifier`` now passes a raising hook, so Cancel interrupts mid-backup.
- **Files vanishing at collect time were invisible even to the skip accounting** (bare ``except OSError: pass``). They are now recorded as OS-error skips and surface in "N file(s) not backed up".

### Fixed — profile store robustness
- **A corrupt main profile file can no longer poison its own .bak**: the .bak refresh is guarded by a parse check, so ``.bak`` always holds the last-known-good version.
- **Unrecoverable profiles (main + .bak both corrupt) are quarantined to ``.json.broken``** — preserved for manual inspection, removed from the active set (no more ERROR on every load), and no longer claimable by a fresh wizard run.
- The dedup docstring no longer claims "keeps newest" (it keeps the first file in name order; only reachable via manual file copies).

### Removed — dangerous dead code
- ``src/core/phases/encryptor.py``: an unused, non-atomic twin of the hardened encrypted writer (no ``.partial``, no failure cleanup) that invited rewiring.
- ``src/security/verification.py``: an unused verifier that hashed the SOURCE paths — wired up, it would have been a fake verifier for local backups. Both removed from the self-integrity APP_FILES list and the coverage config.

### Added — update notifications
- **The GitHub release checker is now wired into app startup** (it was fully implemented but had zero callers). A newer stable release raises a non-blocking tray notification; the check is HTTPS-only, 10 s timeout, silent on failure.

### Fixed — S3 & wizard hardening
- **The encrypted-S3 archive upload is now cancellable and reports progress** (the raw client path had no Callback, so Cancel was dead for the whole upload). Multipart sizing moved to an explicit ``TransferConfig``; the client-level ``multipart_chunksize`` key — silently ignored by boto3 — was removed.
- **New least-privilege IAM policy builder** scopes bucket/object actions to the actual backup buckets instead of ``arn:aws:s3:::*`` (the broad bootstrap policy remains only for pre-bucket setup).
- **SMTP logins to a remote server with TLS off are upgraded to STARTTLS opportunistically** when the server advertises it (loopback bridges like ProtonMail's stay exempt), so credentials no longer cross the wire in cleartext on misconfigured custom servers.
- Wizard cost estimator: fixed the ×1000 LIST pricing typo, reconciled the diff-size figures, and documented that the Glacier-IR table is a lower bound while uploads land in STANDARD.
- The plaintext-HTTP geolocation call is documented as a bounded, non-sensitive exception (suggestion-only, timezone fallback unaffected).

### Fixed — UI & scheduler accuracy
- **Switching the sidebar to a profile whose backup is already running now shows its live progress** instead of a dead 0% bar (raise-only seed of the live-view flag from the engine registry — the cross-tab contract is preserved).
- **The startup catch-up no longer backdates ``last_trigger`` and the in-flight marker** by hours: both are stamped at the actual trigger moment.
- **A session-end marker is logged when the window is destroyed** (OS shutdown with the app in the tray was indistinguishable from a crash — 65 of ~150 sessions).
- Encryption module docstring updated to the real v2 format (HMAC trailer, split-key derivation); the user-scope DPAPI tradeoff is now documented explicitly.

### Fixed — stores & throttling
- **Rotation now prunes the rotated archive's entry from ``verify_hashes.json``** (profiles without mirrors — the reference is shared by name across destinations), stopping the signed store's unbounded growth.
- **``run_history`` JSONL files are compacted on disk** once they exceed twice the load cap (the cap was previously load-only and the files grew forever).
- The SFTP exec-channel bandwidth throttle uses the same 1-second sliding window as every other throttle path (no more burst-pause divergence on long uploads).

### Deferred (documented in code)
- The differential filter still re-hashes every unchanged file (perf-only; correctness confirmed exact). Any change requires a real USB benchmark per the project's perf invariants — marked TECH-DEBT in ``filter.py``.
- Threading ``SecurePassword`` through the encryption path is deferred with rationale (the str-based key-derivation flow is perf-critical).

### Tests
- +45 tests across in-memory-manifest verification, verify cancellation, collector skip accounting, .bak/quarantine behaviour, update-notice wiring, S3 upload callback, IAM scoping, STARTTLS policy, run-tab live-view seeding, store pruning and history compaction.
- Full suite: 2775 passed, 0 failed, 28 skipped.

## [3.7.52] - 2026-06-10

Clears the ENTIRE remaining medium-severity backlog of the 2026-06-10 deep audit: 22 fixes
across profile persistence, scheduling, rotation, remote backends and UI safety. The audit
backlog now holds low-severity findings only. Full suite: 2746 passed, 28 skipped.
First release compiled with Nuitka (native C binary) instead of PyInstaller.

### Fixed — profile persistence
- **Concurrent saves could publish a torn profile file.** ``save_profile`` is reached from the Tk, scheduler and backup-worker threads, all sharing one deterministic ``.json.tmp`` per profile. All disk mutations are now serialized by a manager-wide lock.
- **The .bak recovery clobbered the live profile with a non-atomic copy.** Recovery now re-parses the live file first (a concurrent save that already fixed it wins) and restores via ``.tmp`` + ``os.replace``.
- **Switching storage type silently wiped the other type's configuration.** ``_build_storage_config`` now carries over every non-selected-type field, so an SFTP→S3 switch keeps the SFTP settings and stays reversible. (LOCAL↔NETWORK share ``destination_path``, which cannot be preserved.)

### Fixed — scheduling & precheck
- **Precheck failure details were discarded everywhere.** Each failed target is now logged at WARNING (profile, role, action, detail) and the detail line is rendered in the "Destinations unavailable" alert.
- **A single transient probe failure cost the day's scheduled backup.** The scheduled precheck now retries with a 5/15/30 s backoff ladder (stop-aware) before involving a human — the manual path's 500 ms retry was never enough for an unattended machine.
- **The retry ladder re-prompted the user up to 5 more times after an explicit Cancel.** ``_retry_backup`` now classifies skip/cancel-class exceptions like ``_trigger_backup`` and abandons the ladder; an explicit Cancel raises the new ``PrecheckUserCancelledError`` (journalled "cancelled", never retried).
- **A manual backup launch destroyed the scheduler's pending precheck prompt, stranding it for 30 min.** The alert frame now tracks its owner; a displaced scheduler prompt is detected via ``<Destroy>`` and re-prompts instead of timing out silently.
- **Queued profiles left by a cancelled chain fired hours later from an unrelated run's drain.** ``_on_precheck_cancel`` clears the queue; a completed run also drops its own stale queue entry.
- **drive_serial: a single 5 s PowerShell stall faked an unplugged drive.** Full-system enumeration now gets a 10 s budget plus one retry.

### Fixed — concurrency state (UI)
- **The single ``_backup_running`` boolean was cleared by the FIRST of two overlapping runs to finish**, un-blinding the save guard and the health-probe race shields for the still-running backup. Every reader now uses a predicate derived from the per-profile ``_active_engines`` registry.
- **An overlapping queue drain could release the WRONG profile's run slot on cancel** (leaking the cancelled one until restart). The cancel closure now binds its own profile id, and drains are skipped while a launch is mid-precheck.
- **Per-run logs cross-contaminated under concurrent runs.** Engines sharing one EventBus captured each other's LOG lines into their per-run files (History misclassification). ``_capture_log`` now drops events tagged with a different ``profile_id``.

### Fixed — rotation & cleanup
- **GFS rotation trusted backend mtime exclusively**: ``modified=0`` meant unconditional deletion and a future mtime made a backup immortal. Rotation now parses the timestamp embedded in the backup NAME (mtime is only a fallback) and KEEPS any backup it cannot date.
- **Abandoned ``.tar.wbenc.partial`` files leaked on the destination forever** (a hard kill mid-write left them invisible to every cleanup path). The orphan scan now sweeps a profile's ``*.partial`` files older than 1 h, and the incomplete-backup cleanup tries the ``.partial`` names too.
- **The pre-write disk-space check ran against the unresolved drive letter and silently skipped unreachable destinations.** It now resolves the USB drive's current letter, records a visible warning when space cannot be verified, and scales the margin with file count for tar/encrypted destinations.

### Fixed — remote backends
- **A missing remote directory was indistinguishable from an empty backup**: SFTP ``_list_files_exec`` discarded ``find``'s exit status. Non-zero now raises with the stderr detail (closes the stage-5 family's listing leg).
- **Every UI/health/precheck "test connection" left an immutable probe object in the Object Lock bucket.** The write probe is now gated on the bucket's Object Lock flag (known at construction), not on the run-time retention date.
- **A transient S3 failure could record a TRUNCATED upload as success**: retries re-sent a partially-consumed stream (s3transfer resumes from the current position). ``with_retry`` now snapshots and rewinds seekable stream arguments between attempts; non-seekable streams are rejected up front.
- **delete_backup "succeeded" on Object Lock buckets via delete markers**, hiding backups from the app while every version stayed billed. It now refuses with an explicit error (deletion is lifecycle-only).
- **A failed prefix-stats call fabricated a 0-byte 1970 backup entry** that rotation would have deleted on sight. The entry is now skipped from the listing (self-healing on the next successful stat) and the real error is logged.

### Fixed — UI safety & noise
- **The destination health poll ran a full SSH handshake every 60 s, 24/7** (~91% of the log; plausible contributor to the Winsock buffer exhaustion that killed a session). Polling now pauses while the window sits in the tray, probes remote backends only every 5th tick, and logs destination state TRANSITIONS at WARNING/INFO instead of burying failures at DEBUG.
- **Enter triggered the destructive Confirm app-wide on inline panels** — the exact accident the cancel-first design claims to prevent. On destructive panels Enter now cancels; the panel's app-level key bindings are removed at teardown (they used to survive ``destroy()`` forever).

### Tests
- +65 tests across config concurrency/recovery, storage-tab type switching, precheck retry classification, capture-log filtering, name-dated rotation, stale-partial sweeping, disk-space verification, health-poll throttling, SFTP exit-status, S3 Object-Lock/rewind/stats, and confirm-panel key safety.
- Full suite: 2746 passed, 0 failed, 28 skipped.

## [3.7.51] - 2026-06-10

Batch fix of 13 medium-severity findings from the 2026-06-10 deep audit — the data-compromise
subset plus the scheduling-reliability and security twins of earlier fixes. Full suite:
2681 passed, 28 skipped, 93% coverage.

### Fixed — backup integrity
- **"Verification OK (GCM-authenticated)" for local encrypted backups authenticated nothing.** When post-backup verification is enabled, the ``.tar.wbenc`` archive is now actually streamed through the decryptor (per-chunk AES-256-GCM tags + trailing HMAC, new ``encryption.verify_encrypted_archive``) — a corrupt, truncated, or tampered archive now fails verification instead of passing on a size ``stat``. When no password is available, the log is honest about what was checked.
- **The differential reference manifest recorded vanished files as backed up.** A file that vanished mid-write was pruned from the backup (3.7.50) but still entered the delta manifest via ``ctx.file_hashes`` — an identical re-creation was then skipped by every future differential, silently never backed up. ``_phase_update_delta`` now excludes ``skipped_files`` entries.
- **Remote backends ignored ``.wbcommit`` entirely.** SFTP and S3 ``list_backups`` now hide uncommitted/partial backups when the remote directory uses commit markers (matching ``LocalStorage``), so an interrupted upload can no longer be listed as restorable, rotated, or "verified". Legacy directories without any marker still list everything (no genuine backup is hidden). New shared helper ``base.backup_base_name``.
- **Periodic remote verification said "ok" for an empty or missing backup** (inode-size fallback). Already hardened in 3.7.50; this release extends the same emptiness rules to the encrypted-archive branch (zero-byte archive → ``missing``).

### Fixed — secrets / profiles
- **A transient DPAPI failure destroyed stored secrets on the next save.** ``_unprotect_secrets`` now returns the original encrypted blobs of fields that failed to decrypt; ``save_profile`` writes those blobs back verbatim instead of re-encrypting the empty placeholder. When DPAPI recovers, the secret decrypts again — nothing is lost.
- **A NETWORK profile vanished permanently after a DPAPI failure.** ``StorageConfig.validate()`` no longer requires the *decrypted* ``network_password`` (a runtime secret): a decryption failure no longer classifies the profile as "corrupted" (which also defeated the ``.bak`` fallback, identically encrypted). Structural fields (destination, username) are still validated; password presence is enforced at UI-input time.

### Fixed — rotation
- **One profile's rotation could delete another profile's backups on a shared destination.** The rotation/count filter was a bare name prefix, so "My Backup" (prefix ``My_Backup_``) matched "My Backup v2" backups. Replaced with a strict ``<name>_(FULL|DIFF)_<timestamp>`` name-boundary match (new ``local_writer.backup_belongs_to_profile``), used by both the rotator and the post-backup "backups available" count.

### Fixed — scheduling reliability
- **Missed WEEKLY/MONTHLY slots were never caught up.** ``_is_due`` hard-gated on the exact scheduled day, so a slot missed because the PC was off that day was skipped for the whole week/month. It now compares the last trigger to the most recent scheduled occurrence (new ``_most_recent_weekly`` / ``_most_recent_monthly``), catching up a missed slot on the next launch without suppressing the next on-schedule run.
- **A torn scheduler-state write reset all state and mass-retriggered every profile.** ``scheduler_state.json`` / ``schedule_journal.json`` were written non-atomically and reset to empty on any parse error, losing every ``last_trigger`` (so ``_is_due`` then saw ``last=None`` and fired every profile). Writes are now atomic (``.bak`` + fsync'd ``.tmp`` + ``os.replace``) and load falls back to ``.bak`` before resetting.
- **A clean app exit during a retry wait permanently abandoned the failed backup.** The in-flight marker was always cleared in ``_trigger_backup``'s finally. ``_retry_backup`` now reports whether retries were aborted by shutdown, and the marker is preserved in that case so the next startup catch-up re-runs the backup.

### Fixed — security
- **Path traversal on S3 directory restore.** ``download_backup`` wrote each object by key with no containment check; a malicious key (``../``, absolute) escaped the restore directory. Added the same resolved-path containment guard as the SFTP tar-slip fix (3.7.50).
- **Object Lock retention was never applied to S3 mirrors.** Retain-until was set only on the primary backend, so an Object-Lock mirror bucket got no per-object retention. A per-mirror ``apply_object_lock`` callback now applies it — only on Object-Lock mirror buckets, so a plain mirror bucket is never sent a retain-until S3 would reject.

### Fixed — notifications
- **SMTP authentication and connection failures were completely silent.** Those two branches returned a tuple callers routinely discarded, with no log line, so an expired app-password killed every notification invisibly. Both now log at ERROR.

### Tests
- +69 tests across encrypted-archive authentication, delta-manifest pruning, remote commit-marker filtering, secret-decryption resilience, rotation name boundaries, retry-abort marker preservation, S3 restore traversal, mirror Object-Lock, weekly/monthly catch-up, atomic scheduler state, and SMTP failure logging.
- Full suite: 2681 passed, 0 failed, 28 skipped, 92.96% coverage.

## [3.7.50] - 2026-06-10

Batch fix of the 13 highest-priority "high" findings from the 2026-06-10 deep audit
(the 5 criticals shipped in 3.7.49). Full suite: 2636 passed, 28 skipped, 92.7% coverage.

### Fixed — data loss
- **Orphan scan deleted other profiles' in-flight backups on a shared destination.** `_phase_orphan_scan` now only deletes orphans whose name carries this profile's `sanitize_profile_name(...) + "_"` prefix (mirrors the rotator). Foreign / other-profile artefacts are never touched. Fixes the 18/05/2026 incident where a concurrent run wiped TestLoic's 2.36 GB in-flight backup mid-write.
- **Mirror backups were never committed when `auto_verify=False` (the default).** `_commit_mirror` was only reachable inside `_phase_verify_mirrors`, which returned early when verification was off — so mirror uploads got no `.wbcommit` and the next run's orphan scan deleted them all. Commit is now decoupled from verify: every successfully-uploaded mirror gets its marker regardless of `auto_verify`; a mirror that fails verification still gets none.
- **A profile-switch auto-save overwrote engine-written run-state with a stale cached value.** `_save_profile` now refreshes engine-owned fields (`last_backup`, `last_full_backup`, recovery flags, `profile_hash`, ...) from disk before writing (`_preserve_engine_owned_state`), so a UI save can no longer regress what the scheduler just persisted (10/06/2026: crypter's `last_backup` written back two days stale).
- **`save_profile` persisted an unvalidated StorageConfig.** The storage tab builds its config by assigning `storage_type` via direct attribute set, bypassing `__post_init__` — so a half-configured remote profile (SFTP without host, S3 without bucket) reached disk and was rejected as "corrupted" on the next load, silently rolling back to `.bak` and discarding the edit (12/05 + 28/05 incidents). `save_profile` now validates (tolerating the default placeholder), and the UI surfaces the error inline on the Storage tab.

### Fixed — false "success"
- **A scheduled backup the user cancelled was journalled `success`.** The cancel branch now records `status="cancelled"`, saves the per-run log, and re-raises; the scheduler classifies `CancelledError` as a skip (no retry storm, terminal "cancelled" status) instead of falling through to its success path (14/05/2026).
- **A backup whose every source was unreachable reported a green 0-file success.** All configured sources missing now raises `RuntimeError` (a dead source drive must fail, not produce "successful" empty runs forever). One missing source among several present ones surfaces a warning and backs up the rest. A genuinely-empty existing source still succeeds.
- **A partial encrypted backup reported unqualified success.** The encrypted-write path now compares the written-file hashes to the input set, prunes the integrity manifest for any vanished file, surfaces a warning, and corrects the reported file count.
- **Periodic remote verification reported `ok` for an empty or missing backup.** It fell back to `get_file_size()` on a backup directory, which returns the inode size (~4096 B). It now requires a non-empty `list_backup_files()` listing; an empty/zero-byte remote backup is reported `missing`.

### Fixed — robustness
- **A single source file vanishing mid-run aborted the whole backup.** `build_integrity_manifest` and `write_flat` now skip a source that vanished (recording it under `skipped_files` and pruning the manifest) instead of raising — the 06/05/2026 (.ico, 256k-file run) and 18/05/2026 (vi.msg, WinError 2) incidents. Genuine I/O errors (permission, disk) still fail fast.
- **Tk-callback and worker-thread exceptions vanished in the windowed build.** `main()` now installs `root.report_callback_exception` and `threading.excepthook`, both routing to the logger, so a crash leaves a diagnosable trail in `backup_manager.log` instead of being written to a null stderr.
- **The scheduler thread could be pinned for the full 30-minute precheck-prompt timeout, including across app exit.** The prompt wait is now stop-aware (`InAppScheduler.is_stopping()`) and releases promptly on shutdown. (Sequential one-at-a-time backups remain by design.)

### Fixed — security
- **Tar-slip / path traversal on SFTP directory restore.** `_extract_tar_members` now rejects any member whose resolved path escapes the restore directory (absolute paths, `..` segments) before writing it.
- **Object Lock retention was bypassed on encrypted S3 uploads.** The encrypted-tempfile upload path went through a raw boto3 client with no `ExtraArgs`, so the archive received only the bucket default retention. It now forwards `_build_lock_extra_args()`, restoring the per-object retention guarantee (a full outlives the differentials referencing it).

### Tests
- +50 regression tests across orphan-scan prefix filtering, mirror commit decoupling, run-state preservation, save validation, cancel classification, missing-source failure, encrypted partial pruning, periodic-verify emptiness, vanished-file skip, exception hooks, scheduler stop-awareness, tar-slip containment, and Object-Lock ExtraArgs.

## [3.7.49] - 2026-06-10

### Fixed
- **An empty or failed remote upload could be recorded as a successful backup (the "stage-5" hole).** Incident (14/05/2026): a server-helper shell incompatibility silently dropped the tar stream; the remote directory ended up empty, yet the run was committed and journaled as success with 231,908 files. The mechanism survived the helper fix: ``_verify_remote`` interpreted an empty ``list_backup_files()`` result as "backend does not support file listing" and skipped verification — but SFTP and S3 (the only storage types that reach this code) both implement listing, so an empty list means the upload produced *nothing on the server*, not that listing is unsupported. Worse, the live encrypted-S3 profile hit this branch on **every run since encryption was enabled (28/05/2026)**: the artifact is a single ``{name}.tar.wbenc`` object, so listing the ``{name}/`` prefix is always empty and no AWS upload was ever post-checked.
- **Fix**: ``_verify_remote`` now (1) routes encrypted remote primaries to a new ``_verify_remote_encrypted_archive`` that size-checks the ``.tar.wbenc`` object directly (mirroring what ``_verify_encrypted_archive`` already did for mirrors) and raises if it is missing or zero bytes; (2) raises ``RuntimeError`` when a non-encrypted remote backup has source files but an empty remote listing, instead of silently skipping; the skip path remains only for the genuinely-empty case (no source files). The primary-encryption decision is extracted into ``primary_is_encrypted()`` in ``src/core/phases/writer.py`` — a single predicate shared by the writer and the verify phase so they can never diverge.
- **A committed, verified primary backup was destroyed when any post-commit phase failed or the user cancelled.** Incident (15/05/2026, twice in one day): an SFTP mirror socket error after the primary commit routed through ``run_backup``'s except blocks into ``_best_effort_cleanup``, which unconditionally deleted the day's good primary **including its ``.wbcommit`` marker** — leaving zero backup for the day. The cleanup docstring's correctness claim ("without a ``.wbcommit`` the backup is invisible either way") was false for the post-commit window.
- **Fix**: ``PipelineContext`` gains a ``primary_committed`` flag, set by ``_phase_commit_primary`` on both its success paths (local marker write, remote marker upload) and left ``False`` on failure. ``_best_effort_cleanup`` now refuses to touch the primary when the flag is set — uncommitted mirror artifacts are still reclaimed, and the uncommitted-primary path is unchanged.

### Tests
- +15 regression tests: ``TestVerifyRemote`` (encrypted archive present / missing / zero-byte; empty listing with files raises, without files skips; checksum and size dispatch unchanged), ``TestBestEffortCleanup`` (committed primary never deleted, uncommitted mirrors still cleaned, uncommitted primary still reclaimed), ``TestPhaseCommitPrimary`` (flag set on local + remote success, left unset on both failure paths), ``TestPrimaryIsEncrypted`` (all-flags / each-flag-off matrix).
- Full suite: 2586 passed, 0 failed, 28 skipped, 92% coverage.

## [3.7.48] - 2026-06-02

### Fixed
- **A scheduled profile could silently skip its daily backup if a previous trigger died in flight.** User report (02/06/2026): three profiles shared ``daily at 10:00``. At 13:05 the app launched and the startup catch-up triggered ``crypter`` (first in the list), which advanced its ``last_trigger`` to 13:05 *before* the run; the PC was then powered off before the engine executed a single line. At the 18:26 relaunch ``crypter`` was no longer ``_is_due`` (last_trigger 13:05 ≥ today's 10:00 slot) and its ``last_backup_completed`` was still True (the engine never armed its own crash flag), so the startup catch-up skipped it — while the two other profiles, whose ``last_trigger`` was still the previous day, ran normally.
- **Root cause**: the schedule slot is consumed (``last_trigger`` advanced in ``_trigger_backup``) *before* the backup runs, while the only safety net (``last_backup_completed = False`` + ``incomplete_backup_name``) is armed *later*, by the engine, after its first ``save_profile``. A process death in that gap burns the daily slot with no recovery signal: ``_is_due`` sees the slot consumed and ``crash_recovery_due`` needs a flag that was never written.
- **Fix**: ``InAppScheduler`` now writes a persistent *in-flight marker* (``SchedulerState.set_inflight``, key ``inflight_<id>`` in ``scheduler_state.json``) the moment ``_trigger_backup`` owns the run slot, and clears it only in its ``finally`` — which runs only if the process survives (success or handled failure). A marker that outlives a restart therefore proves a die-in-flight, so ``_check_startup_missed`` forces a catch-up (``orphan_trigger_due``), bounded by the existing crash-recovery circuit breaker. This distinguishes a hard process death (marker orphaned → recover) from a clean handled failure (marker cleared → wait for the next slot, no boot-loop). Scope: scheduler path only; the manual/tray path (which also advances ``last_trigger`` before running via ``mark_triggered_now``) is left as known tech-debt.

### Tests
- +19 tests in ``tests/unit/test_scheduler_inflight_recovery.py``: the ``SchedulerState`` in-flight marker contract (roundtrip, persistence across instances, independence from ``last_trigger``, defensive empty-id and corrupt-value handling); the ``_trigger_backup`` lifecycle (marker present during the run, cleared after success and after a handled exception, never written when the slot is already held); ``_check_startup_missed`` orphan recovery (forces a trigger when the slot is consumed, increments the circuit breaker, clears the marker after a successful relaunch, is blocked when the breaker is maxed, is skipped when a run is already in progress, and stays quiet with no marker); plus a full reproduction of the 02/06/2026 incident (one burned-slot profile recovered alongside two genuinely-due ones) and its inverse contract (without the marker the bug reproduces — guarding against a revert).
- Also removed a pre-existing unused ``import os`` in ``src/core/scheduler.py``.
- Full suite: 2568 passed, 0 failed, 28 skipped, 92% coverage.

## [3.7.47] - 2026-05-30

### Fixed
- **A profile's dashboard "Last backup" card could show "Failed" right after a successful backup.** User report (30/05/2026, v3.7.46): the ``crypter`` card read "✗ Failed — 57min ago" while its own run log showed a complete success (archive written, 2356 files, commit marker, rotation). Distinct from the run-rejection race fixed in 3.7.46 — this one is in the journal.
- **Root cause**: ``ScheduleJournal.update_last()`` updated ``self._entries[-1]`` — the GLOBAL last entry, regardless of profile. When two profiles run back-to-back (the sidebar chain crypter → My Backup → AWS), the ``started`` row of one and the ``success`` of another telescope: crypter's ``success`` (and its 2356-file count) landed on the freshly-appended ``My Backup`` row, and crypter's own entry stayed stuck on ``started``. The dashboard reads ``status == "success"``, so a ``started`` (or stray ``failed``) row painted a successful backup as "Failed". Same mechanism behind the long tail of ``status: "success"`` rows that carry a ``ProfileLockError`` detail.
- **Fix**: ``update_last(profile_id, …)`` now targets the most recent entry for the *given profile*, not the global last entry; all 13 call sites pass their ``profile_id``. ``get_last_run()`` additionally skips non-terminal rows (``started`` / ``waiting``) so an in-flight run or a crash orphan can never be painted as a failed backup — it returns the last entry with a terminal status (success / failed / cancelled / skipped). Existing corrupt rows are therefore ignored too, so the card self-heals without touching the journal file.

### Tests
- +9 tests in ``tests/unit/test_schedule_journal.py``: ``update_last`` updates the matching profile's row and leaves other profiles untouched (the exact crypter/My Backup telescoping), unknown-profile and empty-journal no-ops, legacy global fallback; ``get_last_run`` skips orphan ``started`` / ``waiting`` / ``verify`` rows, returns the last terminal entry, and does not leak across profiles.
- Full suite: 2548 passed, 0 failed, 28 skipped, 92% coverage.

## [3.7.46] - 2026-05-30

### Fixed
- **"Backup rejected: Another backup is already running" could still appear in 3.7.45** — specifically when a freshly-activated profile was caught by the scheduler's catch-up at the same instant the user clicked "Start backup". User repro (30/05/2026): activated ``My Backup``, clicked Start; the manual chain and the scheduler both reached ``run_backup`` for ``My Backup`` at once and the loser logged the rejection. 3.7.45 only coordinated the manual↔manual case (its ``_backup_running`` / ``_launch_in_progress`` / ``_active_engines`` guard); the scheduler used a *separate* registry (``_profile_in_progress``) and neither path consulted the other in time.
- **Root cause**: a freshly-activated profile has ``last_trigger = None``, so ``_is_due`` returns True immediately — the scheduler triggers it on the next tick / startup missed-backup check, racing the manual launch. ``run_backup`` logs "Backup rejected" *before* the ``ProfileLockError`` even propagates, so the message surfaced even though the scheduler already classifies that exception as "skipped (concurrent)".
- **Fix**: a single atomic test-and-set, ``InAppScheduler.try_acquire_profile``, on the shared ``_profile_in_progress`` set, used by BOTH paths. The scheduler's ``_trigger_backup`` now skips the callback entirely (no ``run_backup``, no rejection line) when the slot is already held; the manual ``_precheck_and_run`` claims the slot before launching and, if it cannot, logs ``Backup skipped — already being backed up`` and chains to the next queued profile. The slot is released in the backup thread's ``finally`` (or in ``_on_precheck_cancel`` if the launch is aborted during the precheck), tracked by a new ``_launching_profile_id``.

### Tests
- +7 tests in ``tests/unit/test_concurrent_run_coordination.py``: ``try_acquire_profile`` test-and-set semantics (acquire / second-fails / release-then-reacquire); the scheduler's ``_trigger_backup`` skipping its callback when the slot is held (manual holds → no second ``run_backup``) and running when free; and the manual ``_precheck_and_run`` skipping + chaining when the slot is taken (scheduler holds → no duplicate launch), claiming the slot when free, and releasing it on precheck cancel.
- Full suite: 2539 passed, 0 failed, 28 skipped, 92% coverage.

## [3.7.45] - 2026-05-30

### Changed
- **A backup requested while one is already running is now queued instead of rejected.** User report (30/05/2026, profile ``crypter``): clicking "Start backup" — or double-clicking it, or the daily schedule firing — while a backup for the same profile was still running produced ``Backup rejected: Another backup is already running for this profile in this application.`` and the second request was silently lost. The engine's per-profile lock (``src/core/profile_lock.py``) raised ``ProfileLockError`` and ``BackupEngine.run_backup`` logged it as a rejection. Now ``BackupManagerApp._run_backup`` detects the in-flight (or mid-launch) state and appends the request to the existing ``_backup_queue`` instead; the running backup's ``finally`` drains it via ``_dequeue_next_backup`` when it finishes.
- **Coalescing — at most one pending run per profile.** Re-clicking "Start backup" (or a schedule firing) for a profile that is already running or already queued does NOT stack a second identical full backup. New pure helper ``src/core/backup_queue.py::select_profiles_to_queue`` partitions requested profile ids into "to queue" vs "skipped", deduplicating against the running set (``_active_engines``), the queued set, and duplicates within the request, while preserving sidebar order. Extracted out of the UI so the rule is unit-testable without a Tk interpreter.
- **Double-click race window closed.** The precheck ("Checking destinations...") runs asynchronously, so ``_backup_running`` is not yet True while it is on screen — a fast second click used to slip through and spawn a duplicate run the engine lock then rejected. New ``_launch_in_progress`` flag, set at the start of ``_precheck_and_run`` and cleared in ``_start_backup_thread`` / ``_on_precheck_cancel``, makes ``_run_backup`` queue the second click instead.
- **Scheduled runs now drain the queue too.** A manual click made while a *scheduled* backup was running previously sat in the queue until the next manual click, because only the manual path drained it. ``_scheduled_backup``'s ``finally`` now posts the same ``_dequeue_next_backup`` as the manual path (tracking ``scheduled_failed`` so a failed scheduled run still prompts before chaining the next profile).

### Fixed
- **11 pre-existing broken tests repaired** — independent of the queue change (they failed on ``main`` before this work, verified by stashing the changes and re-running). All were stale after the earlier "one ``BackupEngine`` per run" refactor and the ``RunHistoryStore`` addition, not real product bugs: ``tests/test_write_error_failfast.py`` (5) called ``_start_backup_thread(profile)`` without the now-required ``engine`` argument and built ``__new__`` instances missing ``_active_engines`` / ``_repoll_destinations_after_backup_start`` / ``_save_backup_log``; ``tests/test_run_tab_progress_isolation.py`` (6) built ``RunTab`` stubs without ``_history_store`` (which ``_on_log`` now writes to before the cross-tab gate) and asserted the old direct ``_append_log`` dispatch and argument shape (``_on_log`` now defers to ``_dispatch_log_event``, ``_on_progress`` forwards ``profile_id``).

### Tests
- +18 tests in ``tests/unit/test_backup_queue_coalescing.py``: the pure ``select_profiles_to_queue`` helper (exclusion of running/queued ids, internal-duplicate collapsing, order preservation, empty / all-excluded cases, ``TypeError`` on bad input) and the UI wiring against a real ``BackupManagerApp`` (clicking while ``_backup_running`` queues instead of rejecting, re-click does not stack, a running profile is not re-queued, ``_launch_in_progress`` blocks an immediate start, idle click launches the first profile and queues the rest, the ``_launch_in_progress`` lifecycle, and the dequeue drain).
- Full suite after the change: **2532 passed, 0 failed, 28 skipped, 92% coverage**.

## [3.7.44] - 2026-05-28

### Added
- **"Skipped" status in the History tab** distinguishes a backup that ran but had nothing to do from one that produced a real archive. User observation (28/05/2026): the daily 10:00 ``AWS Backup`` differential run found ``0 changed / 20 unchanged`` files and was correctly skipped by the engine (no S3 object created), but the History tab showed ``Success`` next to it — visually identical to the days that actually wrote a new ``DIFF`` to S3. No way to tell "schedule fired and there was real work" from "schedule fired and nothing to back up".
- **New ``skipped`` status** classified from the dual marker ``"No changes detected"`` AND ``"backup skipped"`` (both substrings must be present — defends against a hypothetical future log line that uses one token in isolation). Displayed as ``Skipped`` in the Status column with the ``Colors.ACCENT`` (blue) tag colour so it stands out from the green ``Success`` and the gray ``—`` (unknown).
- **Precedence rule pinned in the docstring**: ``skipped`` is now checked BEFORE the generic ``"Backup complete:"`` marker because the engine emits BOTH on a no-changes run (the ``"Backup complete: 0 files in 0.0 min"`` epilogue still fires). The existing precedence ``success > cancelled`` (anchored by ``test_success_beats_cancelled_in_reordered_log``) is preserved unchanged.

### Tests
- +4 tests in ``tests/unit/test_history_tab_status.py``: ``test_no_changes_detected_classified_as_skipped`` (the exact AWS Backup 28/05 log shape — both markers present), ``test_skipped_beats_success_when_both_markers_present`` (precedence regression guard), ``test_compound_skip_marker_required`` (a single ``"skipped"`` token in isolation does NOT trigger — protects against a future ``"file skipped due to permission"`` line wrongly classifying the whole run), ``test_skipped_displays_as_skipped_in_treeview`` (end-to-end: the Status column shows ``Skipped`` AND the row carries the ``skipped`` tag for the colour).

## [3.7.43] - 2026-05-28

### Fixed
- **Encrypted backups created with "Verify integrity after backup" UNCHECKED were invisible to the periodic Verify-tab forever.** User report (28/05/2026, v3.7.41 install): selecting the ``tes crypter`` profile in Verify → "Verify all backups" produced a single WARNING row ``"primary | tes_crypter_FULL_2026-05-27_220027.tar.wbenc | No reference hash — cannot verify (71,426,281 bytes)"`` even though the archive was on disk and structurally valid. The Verify-tab on plain (non-encrypted) backups worked, so the bug was specific to the encrypted code path.
- **Root cause** in ``BackupEngine._phase_verify`` (``src/core/backup_engine.py``): the function started with ``if not _effective_auto_verify(ctx.profile): return`` — when ``auto_verify`` was False the function exited BEFORE reaching the ``ctx.config_manager.save_verify_hash(...)`` call that registers the reference SHA-256 for ``.tar.wbenc`` archives. The flag was supposed to skip only the IMMEDIATE post-backup re-read (costly second pass over the freshly-written file), but it also dropped the registration of the reference data point the periodic Verify-tab needs to detect later drift. Pre-3.7.43 the failure was silent: the backup completed, the ``.wbcommit`` marker was written, ``list_backups()`` showed the archive, but ``verify_hashes.json`` stayed empty for that entry. The Verify-tab's ``"No reference hash"`` warning was the only user-visible symptom and only appeared the first time the user opened that tab on an encrypted profile.
- **Why plain backups were not affected**: their reference data lives in the per-file ``.wbverify`` manifest sidecar (written during ``_phase_save_manifest`` regardless of ``auto_verify``). Only encrypted archives, whose manifest is embedded INSIDE the tar, depend on ``verify_hashes.json`` as their external reference — and that write was the one being skipped.
- **Fix**: extract the reference-hash registration into a new ``_register_encrypted_reference_hash`` helper and call it from ``_phase_verify`` BEFORE the ``auto_verify`` early-return. ``auto_verify`` still does its job (skips the post-backup ``"Verification OK"`` log line and the redundant second pass), but the hash + size + timestamp always land in ``verify_hashes.json`` for every ``.tar.wbenc`` written locally. ``_phase_verify`` itself shrinks below 30 lines per CLAUDE.md and the helper carries its own docstring + invariants.

### Tests
- +2 tests in ``tests/test_backup_engine_coverage.py::TestVerifyEncryptedBackup`` closing the coverage hole the bug had hidden in: ``test_encrypted_backup_stores_hash_even_when_auto_verify_disabled`` exercises the exact precondition (encrypted + local + ``auto_verify=False``) and asserts that ``verify_hashes.json`` ends up non-empty with the right key (``.tar.wbenc`` filename) and right size (matches the file on disk). ``test_encrypted_backup_with_auto_verify_false_skips_log_line`` is the symmetric guard: a future refactor that "unifies" the two flows by always logging ``"Verification OK"`` would defeat the original ``auto_verify=False`` user intent (skip the costly second pass). Pre-3.7.43 the first test failed (zero entries in the dict). Post-fix both pass.
- The existing ``test_encrypted_backup_stored_hash`` continues to cover the happy path (``auto_verify=True``) so the change to ``_phase_verify`` does not regress it.

## [3.7.42] - 2026-05-27

### Changed
- **Closed the inline-panel migration that started in 3.7.34.** The remaining 9 ``messagebox.*`` call sites in sub-windows — explicitly deferred by 3.7.34 as "Out of scope" because the tabs / wizard did not hold a reference to ``_main_frame`` — are now all unified on the inline panel pattern. The app no longer opens a native OS dialog during normal use. Every validation warning, every Yes/No prompt, every confirmation now uses the same full-frame ``notify_inline`` / ``confirm_inline`` widget tree the rest of the app already adopted.
- **Sites migrated (9):**
  - ``src/ui/wizard.py:_go_next`` — Step validation error (e.g. "Profile name required") was a ``messagebox.showwarning("Validation", error)`` on top of the wizard ``Toplevel``. Now an inline ⚠ warning panel that scopes to the wizard's content + footer area; the header banner and progress bar stay visible so the user still sees which step they came from.
  - ``src/ui/tabs/history_tab.py`` — 3 sites: ``_open_selected`` OS error opening a log (warning), ``_delete_selected`` Yes/No confirmation (now a destructive-red ``confirm_inline`` panel), ``_delete_selected`` OS error on ``unlink`` (warning). The Yes/No prompt was the original ``askyesno`` from 3.7.30 that was explicitly left for a follow-up release.
  - ``src/ui/tabs/recovery_tab.py`` — 5 sites all in the Restore flow: missing destination, missing local backup, non-existent local backup path, encrypted-but-no-password, missing remote selection. All warnings.
- **Dependency-injection plumbing for the sub-tabs.** ``HistoryTab`` and ``RecoveryTab`` constructors now accept a ``notify_fn`` callback (and a ``confirm_fn`` for HistoryTab); ``BackupManagerApp._build_tabs`` passes ``self._notify`` and the new ``self._confirm`` wrapper at construction time. The tabs do not need a reference to ``_main_frame`` or to the app instance — they just call ``self._notify_fn(title=..., body=..., level="warning")`` like they would call any other helper.
- **New ``BackupManagerApp._confirm`` wrapper.** Mirrors ``_notify`` for the Yes/No case: thin sleeve around ``confirm_inline`` that supplies ``self._main_frame`` + ``_hide_main_layout`` / ``_restore_main_layout`` callbacks so sub-tabs can request a Yes/No prompt with a one-line call. Returns ``True`` if the user confirmed, ``False`` on Cancel / Escape / window close.
- **Wizard internal restructuring.** ``content_outer`` and the navigation ``footer`` frames are now stored as ``self._content_outer`` / ``self._footer_frame`` attributes so two new helpers ``_hide_content_for_panel`` / ``_restore_content_after_panel`` can ``pack_forget`` + re-``pack`` them around an inline panel. Same hide / restore symmetry as ``_hide_main_layout`` in ``app.py``, scoped to the ``Toplevel``.

### Tests
- +17 tests in ``tests/unit/test_subwindow_notify_migration.py``: zero remaining ``messagebox.show*`` / ``askyesno`` call (source-level grep guard on each of the 4 migrated modules — comments and docstrings that mention "messagebox" for historical context are explicitly tolerated, real calls are not), constructor signatures accept the new ``notify_fn`` / ``confirm_fn`` kwargs with ``None`` defaults (preserves legacy test instantiations that didn't need the callbacks), ``BackupManagerApp._build_tabs`` wires ``self._notify`` to ``RecoveryTab`` and both ``self._notify`` + ``self._confirm`` to ``HistoryTab`` (a regression that drops one would cause a NoneType-not-callable on the next validation), ``_confirm`` wrapper exists and returns ``result.confirmed``, each of the 9 migrated call sites carries ``level="warning"`` (except the destructive delete-log confirm which carries ``destructive=True``), and the wizard hide/restore helpers ``pack_forget`` BOTH the content and the footer (a future refactor that drops one would leave the wizard half-broken — content hidden forever after a validation error).
- **2 existing tests updated** in ``tests/unit/test_history_tab_status.py``: ``test_delete_removes_file_and_row`` and ``test_delete_aborted_when_user_declines`` previously patched ``messagebox.askyesno`` which is no longer called. Now they inject a stub on ``history_tab._confirm_fn`` directly. Same coverage intent (auto-confirm True → file removed, decline False → file kept), updated to the new injection pattern.

### Out of scope for this release
- 2 sites in ``history_tab.py`` (``_open_selected`` OS error, ``_delete_selected`` OS error on unlink) were validated by source-inspection tests rather than interactive UI repro — they fire only when ``os.startfile`` / ``Path.unlink`` raise ``OSError``, which requires Windows-specific conditions (broken file association, locked file, lost ACL) that are awkward to reproduce on demand. The migration code path is identical to the 7 other interactively-validated sites (``self._notify_fn(title=..., body=..., level="warning")``), and the source-level grep + level assertions in ``test_subwindow_notify_migration.py`` confirm the call shape matches.

## [3.7.41] - 2026-05-27

### Fixed
- **Restoring an encrypted multi-source backup placed the FIRST source's files flat at the restore root** — every subsequent source was correctly nested under its own folder, but the first one was extracted without its enclosing directory. User report (27/05/2026 on the v3.7.40 install, profile ``tes_crypter`` with two sources ``F:/Documents/Divers/Economie`` and ``F:/Documents/Divers/BFM``): after restore, ``Economie/*`` files sat directly under the restore directory next to a correctly-nested ``BFM/`` folder. Asymmetric and confusing — the user had to manually recreate the ``Economie/`` folder by moving files around.
- **Root cause** in ``RecoveryTab._decrypt_and_extract`` (``src/ui/tabs/recovery_tab.py``): the pre-3.7.41 code learned a ``strip_prefix`` from the FIRST tar member it saw (``"Economie/"`` here, derived from ``member.name.split("/")[0] + "/"``) and unconditionally stripped that prefix from every subsequent member's name. ``Economie/*`` matched and got stripped flat at the root; ``BFM/*`` did NOT match the ``"Economie/"`` prefix and survived intact — hence the visible asymmetry. The code dated back to a single-source assumption where the wrapping folder was an artefact to remove. Once multi-source backups became common, the strip stopped being a clean-up and started destroying user data structure.
- **Fix**: drop the ``strip_prefix`` heuristic entirely and use ``member.name`` verbatim. ``collector.py::add_file`` (line ~336) already builds every tar entry as ``f"{source_root.name}/{inner_rel}"`` — the tar IS correct on disk, the extraction was the one corrupting it. Post-fix, restoration mirrors the structure of the LOCAL non-encrypted path at ``_do_local_restore`` line ~1615 which uses ``f.relative_to(src)`` to preserve every source-folder name. Crypto vs non-crypto restorations now produce identical layouts.

### Tests
- +5 tests in ``tests/unit/test_decrypt_extract_multi_source.py`` — they build a real ``.tar.wbenc`` via the production ``EncryptingWriter`` (no mocks, because the failure mode was integration-shaped and mocks would have missed it again): 2-source archive must keep BOTH top-level folders (the exact user-reported scenario), explicit negative assertion that the first source's files do NOT appear at the restore root, 3-source generalisation, single-source non-regression (wrapping folder still preserved — matches the LOCAL non-encrypted behaviour), and wrong-password rejection unchanged (the fix sits in the same function, this anchors that the friendly ``"The password you provided is incorrect"`` error path is untouched).

## [3.7.40] - 2026-05-27

### Fixed
- **Saving a profile silently switched the user to a DIFFERENT profile.** User report on v3.7.39 install: on the "My Backup" profile, General tab, clicking Save flashed the ``✓ Profile saved`` panel and then bumped the sidebar selection to "AWS Backup" (the first active profile in the list). The tabs reloaded with AWS Backup's content. The user lost their place. Root cause in ``BackupManagerApp._save_profile``: after a save, ``_load_profiles()`` was called WITHOUT the ``select_first=False`` kwarg, so the listbox repopulation auto-selected the first active profile via the default branch (``_load_profile(first_active_profile)``). Symptom-free for the user who only had one profile or saved the first active one — visible the moment a non-first profile was being edited.
- **Fix**: ``_save_profile`` now passes ``select_first=False`` to ``_load_profiles`` AND immediately re-selects the profile that was just saved via the existing ``_select_profile_in_sidebar(profile)`` helper. The user's sidebar selection, listbox highlight and active tab all stay anchored on the profile they were editing.

### Tests
- ``tests/unit/test_app_load_profiles_select_first.py::TestStartupCallersKeepDefault::test_save_profile_does_not_force_false`` was pinning the OLD (bugged) behaviour — replaced with ``TestCallSitesPassSelectFirstFalse::test_save_profile_passes_select_first_false`` that asserts BOTH the ``select_first=False`` call AND the ``_select_profile_in_sidebar(profile)`` re-anchor. The check is a static source-inspection guard so a future refactor cannot silently re-introduce the swap.

## [3.7.39] - 2026-05-27

### Fixed
- **v3.7.38 Save-bar fix was incomplete — the bar still disappeared after clicking Save.** User report on the v3.7.38 install (confirmed via process inspection: PID started 20:27:34, "Saved profile AWS Backup" logged at 20:27:42, Save bar still invisible). The v3.7.38 ``_restore_main_layout`` packed ``_save_frame`` first then the notebook with ``before=self._save_frame`` — equivalent in theory but in practice Tk's pack manager did not honour the inverted insertion the same way when the parent had been ``pack_forget``-ed earlier (the slave list mutation interacted with the previous ``forget`` state).
- **Fix**: switch to the EXACT recipe already proven in ``_on_tab_changed`` (line 1017) — pack the notebook FIRST with ``expand=True``, then pack ``_save_frame`` with ``before=self.notebook``. The ``before=`` placement on the save_frame (not on the notebook) makes Tk insert save_frame FIRST in the slave list; ``side="bottom"`` widgets are evaluated in pack-order and reserve their slot, and only then the remaining cavity goes to the ``expand=True`` notebook. The on-disk comment at line 1008-1016 documents the same trap with the right recipe; ``_restore_main_layout`` now mirrors it verbatim.

## [3.7.38] - 2026-05-27

### Fixed
- **Save bar (bottom blue button) disappeared after clicking Save.** User report on v3.7.37: after clicking the Save button on the General tab, the inline notification panel ``✓ Profile saved`` auto-dismissed correctly — but on the way back the bottom Save bar was gone. The form was visible, the tabs were visible, but the blue Save bar at the bottom had vanished. Same bug would have affected every other call site of ``confirm_inline`` and ``_notify`` (Delete profile cancel, validation warnings, etc.) — the user just hit Save first.
- **Root cause** in ``BackupManagerApp._restore_main_layout``: the helper repacks notebook + save bar after an inline panel closes, but did it in the wrong order:

  ```python
  # WRONG: notebook eats all the space, save_frame has nowhere to land
  self.notebook.pack(fill="both", expand=True)
  self._save_frame.pack(side="bottom", fill="x")
  ```

  ``pack`` allocates space to ``expand=True`` widgets AFTER the fixed-size siblings. Packing the notebook first told Tk "give this widget every pixel"; the save bar packed afterwards on ``side="bottom"`` could not fit and was invisible. The original ``_build_ui`` packing (line 982) gets it right via ``before=self.notebook``; ``_restore_main_layout`` was missing the symmetric recipe.

- **Fix**: pack the save bar FIRST (reserves its bottom slot) THEN the notebook with ``before=self._save_frame``:

  ```python
  self._save_frame.pack(fill="x", side="bottom")
  self.notebook.pack(fill="both", expand=True, before=self._save_frame)
  ```

  Mirrors the recipe already used by the three other restore call sites (close About, close Bug report, close Ready panel).

## [3.7.37] - 2026-05-27

### Fixed
- **``Start backup`` / ``Cancel`` buttons clipped at the bottom of the Run tab.** User report on the v3.7.36 install: when the selected profile was differential (e.g. ``AWS Backup``) the ``Last backup`` card grew to **three lines** (status + source size + ``Last full: …``) instead of two; the extra ~20 px pushed the button row off the bottom of the window, leaving only their tops visible. Root cause in ``src/ui/tabs/run_tab.py:_build_ui``: the log frame was packed with ``side="top", expand=True`` BEFORE the button frame was packed (also ``side="top"``). Tk's pack manager allocates space to ``expand=True`` widgets after the fixed-size siblings, so the log frame ate the remaining vertical space and the button row, packed later, was squeezed against the window edge and clipped when the cards above grew.
- **Fix**: the button frame is now created AND packed with ``side="bottom"`` BEFORE the log frame. Pack order matters here — packing the bottom-side widget first reserves its slot at the bottom of the parent, then the log frame's ``side="top", expand=True`` fills only the remaining space between the alerts row and the buttons. Visual layout is unchanged (log on top, buttons at the bottom) but the buttons are now guaranteed visible regardless of how much the cards above grow.

## [3.7.36] - 2026-05-27

### Fixed
- **MSI re-install on top of a running instance left the next launch silently failing.** User report on the v3.7.35 install: after installing the MSI (which replaces the ``.exe`` on disk), the next double-click of the desktop shortcut produced "nothing visible". Root cause: the MSI ``InstallFiles`` action only replaces the binary, it does not stop the running process. The OLD process (e.g. 3.7.34, possibly sat in the system tray) kept holding the cross-process ``BackupManager_v3_SingleInstance`` mutex. The new binary booted, saw the mutex via ``CreateMutexW`` returning ``ERROR_ALREADY_EXISTS=183``, wrote the ``.show_signal`` fallback file and exited. The running instance was supposed to detect the signal via its 500 ms poll and call ``_show_window`` — but a tray-only instance with no foreground window did NOT visibly raise (a withdrawn root + the user looking at their desktop = no obvious change).
- **Double fix (defence in depth):**
  - **(A) Win32 ``SetForegroundWindow`` from the new instance.** New helper ``_bring_existing_instance_to_front`` in ``src/__main__.py`` enumerates top-level windows via ``EnumWindows``, finds the first one whose title starts with ``"Backup Manager"`` (prefix match so a v3.7.36 launcher still finds the v3.7.34 / .35 window), then calls ``ShowWindow(hwnd, SW_RESTORE=9)`` followed by ``SetForegroundWindow(hwnd)``. The user just clicked the shortcut so we are the foreground process — Windows lets the call succeed and the existing window comes to the front immediately. The signal-file polling stays in place as a belt-and-braces fallback for the rare case where the Win32 raise is blocked (UIPI restriction, hidden+iconic combo, etc.).
  - **(B) MSI ``util:CloseApplication`` before file replacement.** ``build_msi.py`` now adds a ``<util:CloseApplication Target="BackupManager.exe" CloseMessage="yes" TerminateProcess="10000" />`` block. WM_CLOSE is broadcast to every top-level window of the running process so the app gets the chance to save state cleanly, then any window that has not closed within 10 s is force-killed. Scheduling defaults to ``before InstallInitialize``, i.e. before ``InstallFiles`` overwrites the binary — exactly the moment we need. Requires the existing ``WixUtilExtension`` (already linked for the Defender exclusion custom actions); only addition is the ``xmlns:util`` namespace declaration on the ``<Wix>`` root.

### Tests
- +12 tests in ``tests/unit/test_single_instance_raise.py`` covering: non-Windows skip (returns False without touching ``ctypes.windll``), no-matching-window returns False without raising, single matching window calls ``ShowWindow(SW_RESTORE=9)`` then ``SetForegroundWindow`` in that order, prefix-match works across the 3.7.34 → 3.7.36 version range, first-match-wins (enumeration stops at the first hit), empty-title windows are skipped, unrelated "Backup Manager"-like app names do not falsely match the prefix, ``EnumWindows`` exception is swallowed and returns False, ``SetForegroundWindow`` UIPI-style failure does not crash the bootstrap. Plus 2 integration tests pinning that ``_acquire_single_instance`` calls the raise helper BEFORE writing the signal file when ``ERROR_ALREADY_EXISTS`` fires, and does NOT call it on a genuine first launch.

## [3.7.35] - 2026-05-27

### Changed
- **Success / info notifications now auto-dismiss without an OK button.** User report on the v3.7.34 install: after saving a profile, the inline confirmation panel forced a click on the OK button to continue. That's friction the user does not need — the action is done, the panel is just a confirmation. Old toast behaviour (auto-dismiss after a couple of seconds) restored, but inside the same full-frame panel layout introduced in 3.7.34 so the visual consistency is preserved.
- **Per-level interaction policy** in ``notify_inline`` (``src/ui/confirm_panel.py``):
  - ``success``: auto-dismiss after 2.5 s, no OK button rendered.
  - ``info``: auto-dismiss after 3 s, no OK button rendered.
  - ``warning``: stays until clicked — the user MUST acknowledge.
  - ``error``: stays until clicked — same reason. An error that auto-vanished would let the user miss the alert if they looked away for a moment.
  
  All four levels still respect ``Escape`` and ``Enter`` as dismissal shortcuts. In the auto-dismiss modes, a click anywhere on the panel also dismisses early — the user is not held captive while waiting for the timer.
- **New ``auto_dismiss_ms`` parameter** on ``notify_inline`` lets callers override the per-level default: pass an integer ≥ 1 to force an auto-dismiss after that many ms (no OK button shown), pass ``0`` to force click-to-dismiss regardless of level (the OK button is rendered). ``None`` (the default) keeps the per-level policy. Used to keep tests fast without changing production semantics.
- ``_build_notify_panel`` takes a new ``show_button`` flag; when False, the panel binds a ``<Button-1>`` handler on both the outer frame and the centred body so any click on the panel dismisses early. Avoids users feeling held captive while a timer ticks.

### Tests
- +10 tests in ``tests/unit/test_confirm_panel.py``: split the original "every level renders an OK button" coverage into two classes — ``TestNotifyClickToDismiss`` (warning / error render the button and wait) and ``TestNotifyAutoDismiss`` (success / info render NO button and auto-vanish). New cases pin the per-level policy: success / info do NOT render the OK button, success auto-dismisses on its own timer (verified with a short ``auto_dismiss_ms=100`` override), a click anywhere on a success panel dismisses before the timer fires, warning STAYS visible while the user hasn't acted (verified by measuring panel children at 150 ms before the test-injected Escape at 300 ms), and ``auto_dismiss_ms=0`` is the explicit way to force click-to-dismiss on a success-level call. Added validation tests for the new parameter (negative / non-int / bool — Booleans are ints in Python so we reject them explicitly).
- Reused the existing ``TestNotifyVariantStyling`` and ``TestNotifyHideRestoreCallbacks`` classes: switched their dismissal mechanism from "click the OK button" to "press Escape" because the warning level now keeps the button but success / info do not, and Escape works in BOTH modes.

## [3.7.34] - 2026-05-27

### Changed
- **Unified ALL user feedback on the inline panel pattern.** Decision (26/05/2026 review of v3.7.33): every success / info / warning / error message in the app must look like the ``Delete profile`` panel — same icon-coloured header, centred body, single OK button at the bottom-right. The toast bar at the bottom-centre (introduced in 3.7.29) and the remaining ``messagebox.showwarning`` / ``showinfo`` / ``showerror`` calls in ``src/ui/app.py`` are replaced. Net effect: one visual contract instead of three (toast + popup + panel), no more pop-up windows during normal use.
- **New** ``notify_inline(parent_frame, *, title, body, level, button_label)`` in ``src/ui/confirm_panel.py``. Mirror of ``confirm_inline`` but with a single dismissal button (no choice to make, only acknowledgement). Four severity levels map to the icon and its colour: ``success`` ✓ green, ``info`` ℹ blue, ``warning`` ⚠ amber, ``error`` ⛔ red. The OK button is always the neutral accent blue — the action is "close this", never destructive. Synchronous return via ``wait_variable`` so call sites can swap from ``messagebox.show*`` with a one-line edit.
- **New** ``BackupManagerApp._notify(*, title, body, level, button_label, select_tab)`` helper. Thin wrapper around ``notify_inline`` that fills in the main-frame hide/restore callbacks every call site would otherwise repeat. The optional ``select_tab`` parameter switches to the offending tab AFTER the panel dismisses, so a validation error like "Profile name already in use" lands the user on the ``General`` tab to fix the field.
- **17 sites migrated** in ``src/ui/app.py``: the three 3.7.29 toast calls (``Profile saved``, ``Object Lock notice``, ``Modules feature status``) plus 14 ``messagebox.showwarning`` / ``showerror`` validation popups (Backup in progress × 2, Encryption invalid, Profile name duplicate, Destination duplicate, Schedule incompatible, Partial cleanup, No active profile, Invalid storage / mirror config, No profile selected, Description required, Identity document required, Report generation failed). Each migration carries the right severity level so the icon colour matches the message intent. Bug-report sub-panel notifications use ``notify_inline(self._bug_frame, ...)`` directly to overlay inside the bug-report frame without disturbing the main layout.
- ``_build_header`` in ``confirm_panel.py`` now accepts an optional ``icon_color`` argument so notify and confirm panels can both colour the header glyph by severity. ``confirm_inline`` itself now passes ``Colors.WARNING`` for the icon — any confirmation prompt is at minimum a "pay attention" prompt — without re-colouring the icon to red on destructive prompts (the red Confirm button already conveys that).

### Removed
- ``src/ui/notifications.py`` (the ``ToastManager`` + ``Toast`` widget pair shipped in 3.7.29). The bottom-centre toast strip is gone — every former toast call site now opens the inline panel instead. Decision driven by the cohérence-maximale goal: a toast at the bottom of the window was the only visual outlier left after the 3.7.30/.33 inline-panel migrations.
- ``tests/unit/test_toast_notifications.py`` (22 tests). The behaviour they pinned is no longer reachable — notify_inline replaces it end-to-end and is covered by the new test class below.
- The ``self.toasts = ToastManager(main)`` initialisation in ``BackupManagerApp._build_ui``.

### Tests
- +20 tests in ``tests/unit/test_confirm_panel.py`` covering ``notify_inline``: input validation (``None`` parent, empty/non-string title / body / button label, unknown ``level``), end-to-end click round-trip (OK dismisses and returns ``None``, custom button label is used, panel cleaned up on dismiss, Escape and Return are accelerators), per-level icon colour parametrised across the four severity levels (success/info/warning/error), and hide/restore callback ordering with exception isolation. Plus 1 existing test in ``tests/unit/test_backup_type_race.py::test_explicit_save_warns_during_backup`` updated to patch ``_notify`` instead of ``messagebox.showwarning`` (same guard intent, new plumbing).

### Out of scope for this release
- 8 sites in sub-windows (``wizard.py``: 1 site, ``recovery_tab.py``: 5, ``history_tab.py``: 2) still use ``messagebox`` because the tabs / wizard do not currently have a reference to ``_main_frame``. Migrating requires a service-injection pattern on the tab / wizard constructors — to land in a follow-up release.

## [3.7.33] - 2026-05-27

### Changed
- **Backup-deletion progress now renders as a full-screen inline panel** instead of a ``tk.Toplevel`` modal pop-up. User report on the v3.7.32 install: clicking ``Delete profile`` with the ``Also delete backups`` checkbox checked still opened a small ``"Deleting backups — Preparing deletion…"`` pop-up window. That pop-up was a separate widget tree (the legacy ``DeleteProgressDialog``) that escaped both the 3.7.29 inventory of ``messagebox`` calls and the 3.7.30 confirm-panel migration — it pre-dated both and was implemented directly as a ``Toplevel``.
- **New module** ``src/ui/progress_panel.py`` exposes ``InlineProgressPanel(parent_frame, *, title, completion_title, hide_callback, restore_callback)`` with the same ``update(current, total, name)`` / ``complete()`` / ``destroy()`` API as the legacy dialog so the caller in ``_delete_profile_backups_async`` was a 6-line swap. The panel hides the main layout via ``hide_callback`` (same recipe as the confirm panel and About), shows a centred title + 480 px progress bar + counter, marshals every worker-thread call onto the Tk main thread via ``parent.after(0, ...)``, and auto-destroys 500 ms after ``complete()`` so the 100 % state is visible briefly before the layout snaps back. The hide/restore callbacks are matched: a hide that raised does NOT trigger the restore (prevents the UI ending up in an inconsistent state). Idempotent ``destroy()`` (auto-timer + explicit caller close both safe).

### Removed
- ``src/ui/delete_progress_dialog.py`` and its 11 tests in ``tests/unit/test_delete_progress_dialog.py``. The legacy ``Toplevel`` modal was the last pop-up window the app opened during normal use. The contract it tested (``update`` / ``complete`` / ``destroy`` / thread marshalling / name truncation / idempotent teardown) is fully covered by the new ``tests/unit/test_progress_panel.py``.

### Tests
- +23 tests in ``tests/unit/test_progress_panel.py``: construction validation (``None`` parent, empty/non-string title or completion_title), rendered widgets present (Progressbar + title Label), ``update`` semantics (bar value+max set, zero-total tolerated, name shown in title, counter formatted ``N / M``), name-truncation rules (long names truncate keeping the tail with a leading ellipsis at ``_NAME_TRUNCATE_AT``; short names returned verbatim; boundary length unchanged), ``complete`` snaps to 100 % AND swaps the title, idempotent ``destroy`` (double-call safe, panel widget removed from host, auto-destroy timer fires after ``COMPLETION_HOLD_MS`` — verified via Tk event-loop pump), hide/restore callback ordering and exception isolation (restore skipped when hide raised — symmetric protection), and thread-safe ``update`` / ``complete`` from a worker thread without raising.

## [3.7.32] - 2026-05-27

### Fixed
- **Cancel and Delete buttons in the confirmation panel had visibly different heights.** User report on the v3.7.31 install: the ``Cancel`` button was noticeably smaller than the red ``Delete`` button — they did not look like siblings. Cause: v3.7.31 fixed the destructive button visibility by switching it from ``ttk.Button`` to ``tk.Button``, but Cancel was still ``ttk.Button``. The two widget classes have completely different default padding (ttk ~28 px high vs tk ~46 px with our padding overrides), and visually they did not line up.
- **Fix**: both Cancel and Confirm buttons are now ``tk.Button`` with IDENTICAL ``padx`` / ``pady`` / ``font`` / ``relief`` / ``borderwidth``. Only the colours differ — Cancel uses ``Colors.CARD_BG`` + ``Colors.TEXT`` with a light grey border, destructive Confirm uses ``Colors.DANGER`` + white, non-destructive Confirm uses ``Colors.ACCENT`` + white. The shared geometry options are extracted into a single ``common_kwargs`` dict at the top of ``_build_buttons`` so the two widgets cannot drift apart silently in a future edit. The trade-off: the non-destructive Confirm loses the native ``Accent.TButton`` sv_ttk look, but the size parity is worth more than the theme polish on this specific panel (only seen during Delete profile / Backup chain failure prompts).

### Tests
- +5 regression tests in ``tests/unit/test_confirm_panel.py::TestButtonVisualParity`` (class renamed from ``TestDestructiveButtonVisibility`` to reflect the wider scope): both buttons MUST be ``tk.Button`` (a future refactor that re-introduces a mixed ttk/tk pair fails immediately), Cancel and Confirm MUST share every size-affecting option (``padx`` / ``pady`` / ``relief`` / ``borderwidth`` / ``font`` — values captured INSIDE the after-callback to avoid the ``TclError: invalid command`` that ``cget`` raises on a destroyed widget), destructive confirm carries ``bg=Colors.DANGER`` (visibility regression from 3.7.30), non-destructive confirm carries ``bg=Colors.ACCENT``, and Cancel carries ``bg=Colors.CARD_BG`` (must never share the destructive/accent colour — the user has to tell at a glance which button is safe).

## [3.7.31] - 2026-05-26

### Fixed
- **Destructive ``Delete`` button in the new confirmation panel rendered invisible at rest.** User report on the v3.7.30 install: the ``Delete`` button on the Delete-profile inline panel showed no visible text or background — only when the cursor hovered the area did the red hover state appear. Cause: ``confirm_panel.py`` was building the destructive confirm as a ``ttk.Button`` with ``style="Danger.TButton"``, but under sv_ttk (Sun Valley theme) the ttk button layout uses image sprites and ignores custom ``background`` configured via ``style.configure``. Only the ``Accent.TButton`` style ships native sprites; ``Danger.TButton`` is custom and paints nothing.
- **Fix**: the destructive confirm button is now built as a ``tk.Button`` (legacy widget that respects ``bg``/``fg``/``activebackground`` directly), matching the workaround already used for the ``Delete profile`` button in the sidebar (``src/ui/app.py:904``). Non-destructive confirms keep ``ttk.Button`` + ``Accent.TButton`` (sv_ttk native, renders correctly).

### Tests
- +3 regression tests in ``tests/unit/test_confirm_panel.py::TestDestructiveButtonVisibility``: the destructive confirm MUST be a ``tk.Button`` (not ``ttk.Button``) so sv_ttk paints it, MUST carry ``bg=Colors.DANGER`` directly on the widget (a future refactor that re-routes through ``style.configure`` would silently re-break the visibility), and non-destructive confirms MUST stay ``ttk.Button`` so they keep the native Accent look. The 20 existing confirm-panel tests still pass — the helper ``_click_first_button_with_text`` was widened to accept both widget classes.

## [3.7.30] - 2026-05-26

### Added
- **In-app inline confirmation panel** replaces ``messagebox.askyesno`` for the high-stakes Yes/No decisions. The legacy two-step ``Delete profile`` flow (one popup for the profile, a second popup for the backups) collapses to a SINGLE inline panel that shows the whole picture: title, body, an ``[ ] Also delete every backup created by 'X' on E:\Backup Manager, G:\Backup Manager`` checkbox with a "cannot be undone" hint, and Cancel + Delete buttons (red destructive style). The user makes one informed decision instead of clicking through two consecutive modals. Same panel pattern is used for the ``Backup chain failure → run next?`` confirmation, with "Stop chain" / "Run 'NextProfile'" labels.
- **New module** ``src/ui/confirm_panel.py`` exposes ``confirm_inline(parent_frame, *, title, body, confirm_label, ...)`` returning a ``ConfirmResult`` with ``confirmed`` and ``extras`` fields. The function is synchronous (uses ``wait_variable`` under the hood) so existing call sites swap a ``messagebox.askyesno`` line for the new call without restructuring around callbacks. Checkboxes via the ``extras=[ConfirmExtra(...)]`` parameter let one panel collect several boolean decisions in a single screen. ``Escape`` always cancels, ``Enter`` confirms, focus opens on Cancel so a user hammering Enter to dismiss a toast cannot accidentally trigger the destructive action.

### Changed
- ``BackupManagerApp._delete_profile`` (line 1833+): the two consecutive ``askyesno`` popups are now a single ``confirm_inline`` panel with an ``Also delete backups`` checkbox. Object Lock profiles still skip the checkbox (their backups cannot be deleted by the app, only by S3 lifecycle expiry) and the destinations summary is rendered into the checkbox label so the user sees exactly which paths are affected.
- ``BackupManagerApp._dequeue_next_backup`` (line 2641+): the ``Backup failed — run next?`` popup is now a ``confirm_inline`` panel with "Stop chain" + "Run 'X'" buttons. The chain-abort decision deserves more visual presence than a single-line modal.
- Two new helpers ``_hide_main_layout`` / ``_restore_main_layout`` factor the notebook + save-frame hide/restore recipe that ``_show_about`` and the new confirm panel both need. Future inline-panel features only have to call these two helpers — single edit point for layout changes.

### Not migrated (deliberately)
- ``HistoryTab._delete_selected`` keeps ``messagebox.askyesno`` for the ``Delete log file?`` confirmation: the tab does not hold a reference to the main frame the inline panel would attach to. Migrating would require a service-injection pattern (constructor callback or service locator) — out of scope for this release. Lower stakes too (a log file, not a profile).
- ``__main__._handle_hmac_regen_at_startup`` keeps ``messagebox.askyesno`` for the HMAC identity-change alert: it fires BEFORE ``BackupManagerApp`` is constructed, so there is no main frame to host an inline panel. Pre-UI bootstrap modals stay as system popups, by design.

### Tests
- +20 tests in ``tests/unit/test_confirm_panel.py``: input validation (``_validate_args`` rejects ``None`` parent, empty/non-string title / body / labels), dataclass contracts (``ConfirmExtra`` defaults, ``ConfirmResult`` shape), end-to-end click round-trip (confirm → ``confirmed=True``, cancel → ``confirmed=False``, ``extras`` reflect the live checkbox state including toggled-after-mount and default-True preserved-on-cancel cases), ``hide_callback`` / ``restore_callback`` ordering and exception isolation (a raising callback does NOT break the return), and panel teardown (no widget leaked under the host after dismissal). All driven through the real Tk event loop via ``root.after`` scheduled clicks against the shared session-scoped ``tk_root`` fixture.

## [3.7.29] - 2026-05-26

### Added
- **In-app bottom-centre toast notifications** replace ``messagebox.showinfo`` for transient acknowledgements. Saving a profile, removing an Object Lock profile, and listing modules used to fire a modal pop-up the user had to dismiss with a click — now they show a non-blocking toast that auto-dismisses after 2.5 s (success) or 3 s (info). Toasts stack vertically up to 3 visible simultaneously (newest on top, oldest force-dismissed past the cap), can be manually closed via the ``×`` button, and follow the Material Design / Windows 11 convention of bottom-centre placement. Multi-line support via ``wraplength`` (capped at ~520 px) lets the diagnostic toasts (``Modules`` feature status) carry per-line detail without overflowing — the toast collapses to a single line ("All N features available") on the happy path and only expands when something is missing.
- **New module** ``src/ui/notifications.py`` exposes ``ToastManager`` with three shortcuts ``success`` / ``info`` / ``error`` (green / blue / red, with an icon prefix ✓ / ℹ / ⚠). Errors get the longest dwell time (5 s) on the assumption that they are less anticipated than success toasts and need more reading time. A ``clear()`` method dismisses every visible toast at once (intended for future use on profile-switch).

### Changed
- ``BackupManagerApp._save_profile`` (line 1648): the ``"Profile 'X' saved"`` confirmation is now a green toast instead of a modal pop-up. Every save no longer interrupts the user with a click-to-dismiss dialog.
- ``BackupManagerApp._delete_profile`` (line 1844): the Object Lock notice ("Backups on AWS S3 are protected by Object Lock") is now a blue info toast.
- ``BackupManagerApp._show_modules`` (line 3370): the feature-status report is now a toast — single-line success when everything is available, multi-line info toast (capped at 5 entries + "… and N more") when features are missing.

### Tests
- +22 tests in ``tests/unit/test_toast_notifications.py``: construction validation (host required, empty/whitespace/non-string messages rejected, unknown levels rejected), stack-and-evict semantics (up to ``_MAX_STACK=3``, newest at top, fourth evicts the oldest, repack after middle dismissal), dismissal idempotence (auto-timer + close-button both safe to call), variant styling (success=green / info=blue / error=red with correct dismiss durations), placement anchor (bottom-centre via ``place(relx=0.5, rely=1.0, anchor='s')``), and multi-line body rendering. All exercised against the real Tk widget tree via the existing session-scoped ``tk_root`` fixture.

## [3.7.28] - 2026-05-26

### Fixed
- **The 3.7.27 swallow guard was incomplete — the red ``"Destination is read-only or locked"`` card still showed during a backup.** User report (26/05/2026 on the v3.7.27 install, TestNP at 60%): the swallow guard introduced in 3.7.27 correctly blocked NEW spurious results from reaching the card while ``_backup_running == True``, but the card had ALREADY been painted red by an earlier poll (typical case: the user clicked the profile in the sidebar a few seconds before the backup started, ``_load_profile`` fired a health check, that check raced the writer of the previous chained backup and finished red BEFORE ``_backup_running`` flipped — the swallow guard had no reason to fire then). The stale red then persisted until the next 60 s poll tick.
- **Fix**: new helper ``_repoll_destinations_after_backup_start`` re-fires every destination probe IMMEDIATELY after ``_backup_running`` is set to True, both in the manual path (``_start_backup_thread``) and in the scheduler path (``_scheduled_backup``). The spawned threads read the flag at start, take the lightweight path (``shutil.disk_usage``), and produce a clean green ``"X GB free"`` result that repaints the card on top of any stale red. The 60 s scheduled ``_poll_health`` keeps running as before. The swallow guard from 3.7.27 stays in place as defence-in-depth for any further NEW spurious result that might come in during the run.

### Tests
- +14 tests in ``tests/unit/test_health_repoll_at_backup_start.py``: one thread spawned per destination (parametrised over 0/1/2/3/5 destinations), threads are daemon + named with a distinguishable ``HealthRepoll-`` prefix, empty / missing ``_health_configs`` is a no-op, repolled threads use the same ``_check_single_destination`` entry point so the swallow guard still applies, and a source-inspection assertion pins down that the repoll call sits AFTER the ``self._backup_running = True`` line in both ``_start_backup_thread`` and ``_scheduled_backup`` (a future refactor that moves the flag-set will not silently bypass the guard).

## [3.7.27] - 2026-05-26

### Fixed
- **Destinations card flashed "read-only or locked" for ~60 s every time a backup started.** User report (26/05/2026 on the v3.7.26 install): every TestNP / L2 / My Backup run opened with a red ``Destination is read-only or locked`` line in the Destinations card even though the backup itself was succeeding. Visible on every version up to and including 3.7.26 — not introduced by 3.7.26, only surfaced more visibly because the backup chain runs immediately after a sidebar click. Root cause is a known race documented in ``src/core/health_checker.py:130-138``: ``_check_single_destination`` reads ``lightweight = self._backup_running`` ONCE at the start of the health-check thread; ``test_connection`` then runs up to ~15.8 s of USB wake-up backoff; if the backup pipeline flips ``_backup_running`` to True during that window, the eventual ``write_text(".backup_manager_test")`` probe trips ``PermissionError`` against the writer thread which is now actively copying to the same drive. The result lands at ``_on_health_result`` with ``online=False`` and the spurious ``"Destination is read-only or locked"`` error, the card paints red, and stays red until the next 60 s poll tick finally reads ``_backup_running == True`` and switches to the lightweight ``shutil.disk_usage`` path.
- **Fix**: ``_on_health_result`` now applies a narrow race guard before forwarding the result to ``RunTab.update_destination_status``. When ALL three clauses hold — ``_backup_running == True`` AND ``health.online is False`` AND ``READ_ONLY_OR_LOCKED_MARKER`` substring is present in the error message — the result is swallowed (DEBUG-logged) and the previous successful card state stays on screen. The marker is exposed as a module-level constant in ``src/storage/local.py`` so the UI can match the exact pattern produced by ``LocalStorage.test_connection``'s ``PermissionError`` branch without grepping a free-form string. Any non-race failure (drive unplugged, real ACL change, network down) and any failure observed while no backup runs are passed through unchanged.

### Tests
- +16 tests in ``tests/unit/test_health_poll_race_swallow.py``: the swallow truth table (6 parametrised cases over the ``backup_running × online × error`` cube), the substring-match semantics (marker embedded inside a longer message still triggers the swallow; empty error does NOT), the bare-object case (``_backup_running`` attribute genuinely absent — guard treats as False, passes through), and the marker-stability sanity check (end-to-end test that ``LocalStorage.test_connection`` actually surfaces ``READ_ONLY_OR_LOCKED_MARKER`` in its ``PermissionError`` branch — catches drift between the const value and the message format).

## [3.7.26] - 2026-05-26

### Added
- **Alert + transparent recovery on HMAC key regeneration.** Defends against a silent data-loss path discovered in the v3.7.25 audit: the per-install HMAC key signs every ``.wbcommit`` marker, and any silent regeneration (DPAPI ``CryptUnprotectData`` failure, accidental delete of ``%APPDATA%\BackupManager\.integrity_key``, antivirus quarantine, corrupted file) invalidated every previously-signed marker. The next ``_phase_orphan_scan`` on a LOCAL destination then classified every historical backup as an orphan and DELETED it, surfacing only ``Orphan removed`` INFO lines in ``backup_manager.log`` and nothing in the UI. Two-step defence:
  - **Alert before destructive regen.** New ``HMACKeyRegeneratedError`` in ``src/core/exceptions.py`` carries the path of the archived old key. New install sentinel ``%APPDATA%\BackupManager\.integrity_key.installed`` distinguishes a genuine first run (no key + no sentinel → silent generation, expected) from "the key disappeared since last run" (sentinel present + key absent → suspect). ``_get_hmac_key`` archives the soon-to-be-replaced key as ``.integrity_key.legacy_<utc_ts>_<reason>`` before any rewrite (best-effort, never blocks the regen path). ``__main__.py`` catches the exception around ``verify_integrity``, shows a modal blocking dialog (default ``No`` = abort, idempotent — the on-disk state is left unchanged so the next launch re-presents the same alert). If the dialog cannot be displayed, the handler defaults to ``abort`` — refusing to start is safer than silently deleting the user's backups because Tk is broken. ``--allow-plaintext-keys`` suppresses every raise (CLI contract preserved for users with permanently broken DPAPI).
  - **Transparent recovery from legacy archives.** ``read_commit_marker`` tries the current key first (fast path, unchanged for non-regen installs). On HMAC mismatch, iterates ``.integrity_key.legacy_*`` archives via the new ``get_legacy_hmac_keys()`` accessor and re-signs the marker in place with the current key when one matches — backup preserved, next read on fast path. Honest scope: recovery only succeeds when the failures are local to the live key file (corruption, AV touched live only). On a Windows reinstall the archives are wrapped with the OLD DPAPI scope which the new user cannot unwrap; ``get_legacy_hmac_keys`` returns empty and the original alert + abort path applies (a future "import legacy key" CLI is out of scope for this release).
  - **Refactor.** ``read_commit_marker`` split from 40-line monolith into ``_decode_marker_file`` + ``_verify_marker_hmac_or_recover`` + ``_validate_marker_structure`` (each under 30 lines per CLAUDE.md), with the recovery branch isolated in ``_try_validate_with_legacy_keys`` + ``_resign_marker_with_current_key``. ``_get_hmac_key`` split into ``_try_read_existing_key`` + ``_unwrap_dpapi_key_or_raise`` + ``_persist_new_key`` for the same reason.

### Tests
- +17 tests in ``tests/unit/test_hmac_key_regen_alert.py`` covering all 5 regen paths (file absent + no sentinel = silent first run; file missing but sentinel present = suspect; read OSError; DPAPI unwrap fail; malformed file size), the migration case (pre-patch install gets sentinel created on first successful read), idempotence of the alert path (two consecutive raises leave on-disk state unchanged), the DPAPI wrap-vs-unwrap path distinction (wrap fail still raises ``DPAPIUnavailableError``, not the new exception), and the bootstrap message helper rendering with and without a ``.legacy_*`` archive present.
- +11 tests in ``tests/unit/test_commit_marker_legacy_recovery.py`` covering the fast-path skip when the current key matches (no ``get_legacy_hmac_keys`` call, no marker rewrite), single + multi-legacy match scenarios, no-archive and no-matching-archive negative cases, re-sign failure graceful fallback (``os.replace`` PermissionError + ``get_app_hmac_key`` OSError mid-resign both still return the validated payload without corrupting the on-disk marker), and end-to-end archive discovery via real ``.integrity_key.legacy_*`` files written to ``%APPDATA%``.
- **2 existing tests updated** in ``tests/unit/test_integrity_check_dpapi.py``: ``TestDpapiUnwrapFailureRegen::test_unwrap_failure_regenerates_key`` and ``TestMalformedFileRegen::test_malformed_size_regenerates`` were pinning down the now-changed default behaviour (silent regen on unwrap fail / malformed file). They now enable ``_ALLOW_PLAINTEXT_FALLBACK`` first to pin down the same silent-regen path under the opt-in flag, preserving the ``--allow-plaintext-keys`` CLI contract coverage.

## [3.7.22] - 2026-05-22

### Fixed
- **Email report's "Backups available" line disagreed with the rotator's "kept" log line on a destination shared with other profiles.** User report (22/05/2026, v3.7.21 install): the post-backup email for ``My Backup`` reported ``Backups available: 9`` while the same run's log said ``GFS rotation: kept 6, deleted 0``. ``backup_engine._phase_rotate`` set ``ctx.result.backups_available = len(ctx.backend.list_backups())`` — the count of EVERY entry on the destination, including the 3 unrelated items (sidecars and another profile's backups on the shared cipango56 SFTP target). The rotator above had already filtered by ``sanitize_profile_name(profile.name) + "_"`` before counting; the engine was not. Fix: new ``_count_profile_backups(backups, profile_name)`` helper at the top of ``src/core/backup_engine.py`` mirrors the rotator's prefix filter and now feeds ``backups_available``. The email retention section now reads the same number as the log's ``kept`` line.

### Tests
- +8 tests in ``tests/unit/test_backups_available_filter.py``: empty list returns zero, empty profile name disables filtering (defensive fall-back for transient init states), foreign-profile entries on the same destination are filtered out, all-belong-to-profile returns the total, profile names with spaces are sanitised (mirrors ``sanitize_profile_name`` from ``local_writer``), ``"Backup"`` does not match ``"My_Backup_FULL_…"`` (anchor at start, no substring false positives), DIFF backups also match the prefix (the next segment may be ``FULL`` or ``DIFF`` — both belong), and an entry missing the ``name`` key is silently skipped (defensive).

## [3.7.21] - 2026-05-22

### Added
- **Cross-profile email config auto-fill and propagation.** Two UX wins for users who share the same notification target across every profile (the common case):
  - **New profile creation**: if any existing profile carries an email (``username`` non-empty), the new profile is auto-seeded with a deep-copy of that ``EmailConfig`` (SMTP host/port, TLS, username, password, from/to addresses, send_on_success/failure, enabled flag). The wizard's own email step still wins if the user fills it in there. Saves a full SMTP setup on every new profile.
  - **Email saved on an existing profile**: when ``Save`` is clicked on a profile whose email becomes configured (``username`` non-empty), the same ``EmailConfig`` is propagated to every OTHER profile whose email is not yet configured. Profiles that already carry their own email are preserved verbatim — the user has explicitly customised them and we do not overwrite their intent. The propagation is automatic (no confirmation popup) and logged at INFO level with the list of receiving profile names.

  "Configured" is defined by ``EmailConfig.username`` being non-empty (the SMTP authentication username is the strongest "user filled SMTP in" signal — an enabled email without a username would fail to send). Whitespace-only usernames are treated as empty so a stray space cannot lock a profile out of propagation.

  Implementation in ``src/core/email_propagation.py`` (I/O-free, fully unit-testable): ``pick_email_source(profiles)``, ``propagate_email_to_unconfigured(source, others)``, ``is_email_unconfigured(email)``. Caller (``BackupManagerApp._new_profile`` / ``._save_profile``) handles persistence; persistence failures on a propagation target are logged and swallowed so they cannot abort the source profile's save.

### Tests
- +15 tests in ``tests/unit/test_email_propagation.py``: ``is_email_unconfigured`` covers default, whitespace-only, and half-filled (SMTP host set but no username) cases; ``pick_email_source`` covers empty/no-configured/single/multi-configured iterations and the iteration-order contract; ``propagate_email_to_unconfigured`` covers no-op when source is unconfigured, full-EmailConfig fan-out, skip-already-configured, source-by-id exclusion (defensive against unfiltered callers), deep-copy isolation (mutating source after propagation does not leak), iteration-order preservation in the returned list, and the empty-others edge case.

## [3.7.20] - 2026-05-22

### Fixed
- **Daily GFS window now keeps at most ONE backup per calendar day**, fixing a visible divergence between the Retention-tab summary line ``Backups kept: 17`` and the rotator's actual output ``GFS rotation: kept 18`` (22/05/2026 user report on TestNP). Pre-3.7.20 the daily window retained EVERY backup with ``(now - dt).days < gfs_daily`` — so 18 FULL backups spread over 4 days (6 today + 1 yesterday + 4 two days ago + 7 three days ago) all fell within ``gfs_daily=8`` and the rotator kept all 18. The Retention-tab summary computes the count from a "1 per day" assumption, hence the 17/18 mismatch. New semantics: ``_apply_gfs_windows`` groups backups by UTC calendar day (``(year, month, day)`` key) and retains the most recent backup of each day, up to ``gfs_daily`` distinct days — mirroring the weekly window's "1 per ISO week" and the monthly window's "1 per month" for predictability. Two DST tests in ``tests/unit/test_rotator_calendar_edges.py`` were updated to pin the new "most recent wins within a UTC day" contract (their prior assertions covered the old "all-in-window" semantics).

### Tests
- +4 tests in ``tests/test_rotator_edge_cases.py::TestDailyWindowOnePerDay``: six backups on the same day collapse to one (regression guard for the 22/05 report), four distinct days each keep their most-recent backup (mirrors the user's actual distribution), one-backup-per-day setup is unchanged (regression guard against over-pruning a well-behaved schedule), and the window correctly caps at ``gfs_daily`` distinct days.
- **2 existing tests updated** in ``tests/unit/test_rotator_calendar_edges.py::TestDstTransitions``: ``test_dst_spring_forward_does_not_split_daily_window`` is now ``…_keeps_most_recent_of_day`` and ``test_dst_fall_back_does_not_double_count`` is now ``…_orders_correctly_in_daily_slot``. Both retain the original goal (pinning UTC ordering against DST shifts) but their assertions reflect the new "1 per UTC day" outcome.

## [3.7.19] - 2026-05-22

### Fixed
- **3.7.18 fast-fail was rendered ineffective by an upstream slow path in ``drive_serial.resolve_local_path``.** User report (21/05/2026, v3.7.18 install): the "Destinations unavailable" popup STILL took ~60-90 s on a disconnected USB drive. ``backup_engine.create_backend(LOCAL)`` calls ``resolve_local_path`` BEFORE the ``LocalStorage`` instance reaches its own ``_wait_for_drive_online``. ``resolve_local_path`` ran (a) its own copy of the ``(0.3, 0.5, 1.0, 2.0, 4.0, 8.0)`` wake-up backoff (~15.8 s), then (b) spawned PowerShell via ``find_drive_by_serial`` to enumerate every mounted disk looking for the configured hardware serial (~2-5 s). With precheck + silent retry + health-check polling, the user saw five ``Drive not found for serial Y47800CN0JN7T5S (was E:)`` warnings spread over 60 s in ``backup_manager.log``. Fix: a new helper ``_drive_letter_root_present(path_str)`` short-circuits both paths — ``_probe_path_with_wake`` returns False in <100 ms when the drive letter root is missing (instead of burning the 15.8 s budget), and ``resolve_local_path`` skips the PowerShell enumeration entirely when the drive letter is gone (no mounted device can carry that serial). Subdir-only-missing case (drive mounted, configured folder deleted/moved) preserves the original behaviour so a legitimate letter reassignment is still detected.
- **``import os`` and ``import time`` hoisted to module scope in ``src/storage/drive_serial.py``** (same rationale as the 3.7.18 lift in ``local.py``): the local imports inside ``_probe_path_with_wake`` prevented unit-test mocking via ``patch("src.storage.drive_serial.time.sleep", …)``.

### Tests
- +5 tests in ``tests/unit/test_drive_serial_fast_fail.py``: ``_probe_path_with_wake`` fast-fails on missing drive letter (zero sleeps, <1 s), wake-up loop still runs on subdir-only-missing scenario (regression guard), ``resolve_local_path`` skips ``find_drive_by_serial`` when the drive letter is gone, ``resolve_local_path`` still calls ``find_drive_by_serial`` when the drive letter is mounted (subdir reassignment case), and the no-serial + missing-letter combo also fast-fails through the early branch.
- **1 existing test updated** (``test_drive_serial.py::test_resolves_to_new_letter``): the mock now distinguishes the drive letter root (present) from the subdir (missing) so the fast-fail check is correctly bypassed and the legitimate letter-reassignment scenario still exercises ``find_drive_by_serial``.

## [3.7.18] - 2026-05-21

### Fixed
- **"Destinations unavailable" took ~32 s to appear when a USB drive was unplugged** (21/05/2026 user report on the v3.7.17 install). ``LocalStorage._wait_for_drive_online`` could not tell a drive that was *unplugged* from a drive that was just *sleeping*: both cases entered the same backoff sequence ``(0.3, 0.5, 1.0, 2.0, 4.0, 8.0)`` = 15.8 s, then ``_precheck_and_run`` issued a silent retry that paid the cost again → ~32 s before the popup. Fix: introspect the drive letter root (e.g. ``E:\``) BEFORE the backoff. If ``Path("E:\\").exists()`` returns False immediately, the drive is physically unplugged — no amount of wake-up retry will resurrect it; return False in <100 ms. If the root is present but the dest subdirectory is not (the legitimate case the wake-up loop targets — drive freshly mounted, subdir not yet enumerated), the loop runs as before. Net effect: unplugged-drive precheck drops from ~32 s to ~250 ms (initial + silent retry).
- **``time`` is now imported at the module level** instead of locally inside ``_wait_for_drive_online``. The local import was a leftover from an earlier refactor; lifting it lets the wake-up loop be mocked in unit tests via ``patch("src.storage.local.time.sleep", …)``.

### Tests
- +3 tests in ``tests/unit/test_local_storage_wake_up.py``: fast-fail when ``Path.exists`` is universally False (no sleeps, <1 s wall-clock), wake-up loop still runs when only the dest subdir is missing but the drive letter root is reachable (regression guard against the fast-fail check firing on subdir-only-missing), and a sanity check on the existing-dest happy path.

## [3.7.17] - 2026-05-21

### Fixed
- **One-shot fix-up of pre-v3.7.16 run-history files** that already had ``phase=""`` on engine-level messages from when the live persist path had the inference-after-persist bug. v3.7.16 stops the bleeding for new entries; v3.7.17 retroactively heals the entries already on disk so the Run-tab Phase column is populated as soon as the user opens an old profile, no waiting for the JSONL to be naturally rewritten as it ages past the 50 000-line cap. Migration runs once at app startup, walks every configured profile via ``migrate_legacy_phase_tags`` (in ``src/ui/tabs/run_tab.py``), applies the same inference as the live ``_resolve_persist_phase`` (regex match against ``_PHASE_PATTERNS`` + per-profile tracker for unmatched inheritance + terminal-line reset), and rewrites the JSONL atomically via the new ``RunHistoryStore.rewrite`` (``.tmp`` + ``os.replace``). Idempotent: the second launch is a no-op because the matchable empty phases have all been filled. Failures on a single profile are logged via ``logger.exception`` and never block startup.

### Tests
- +4 tests in ``tests/unit/test_run_history.py::TestRewrite``: ``rewrite`` replaces entries verbatim, creates the file when missing, no-ops on empty ``profile_id``, and stays atomic under concurrent ``load`` callers (no torn reads — exercised by 50 ``load`` calls interleaved with 10 full rewrites).
- +10 tests in ``tests/unit/test_run_tab_phase_migration.py``: engine-level messages get the inferred phase, existing explicit tags are preserved verbatim, terminal lines reset the per-profile tracker so the next run does not inherit the previous one's last phase, no-op detection (all-resolved file + only-terminals-empty file both leave ``mtime`` untouched), idempotence (a second invocation never re-writes), multi-profile aggregate count, missing profile silently skipped, empty profile list returns 0, and the rewritten JSONL still parses as one JSON object per line.

## [3.7.16] - 2026-05-21

### Fixed
- **Run-tab Phase column went blank for engine-level messages after a profile-switch reload.** User report (21/05/2026, captured on the v3.7.15 chain): viewing TestNP / My Backup / L2 after the chain finished showed ``Saving manifest…``, ``Writing commit marker…``, ``Updating manifest…``, ``Rotating old backups…``, ``Building integrity manifest…``, ``Copying to Storage…``, ``Backup complete: …`` with an empty Phase cell — even though the live run displayed the correct ``manifest`` / ``commit_marker`` / ``writer`` / ``rotator`` tag. Root cause in ``RunTab._on_log`` (``src/ui/tabs/run_tab.py``): the phase inference (regex match against ``_PHASE_PATTERNS`` + fallback to ``_current_phase``) ran AFTER the ``_persist_log`` call, so the JSONL was written with ``phase=""`` for every engine-level emit. ``_reload_log_history`` (fired on every sidebar click) re-rendered the rows from those persisted entries and the column was blank. Fix: ``_persist_log`` now runs the same inference via a new helper ``_resolve_persist_phase`` BEFORE writing. To stay safe across background scheduler runs that emit LOG events for a non-viewed profile, a per-profile tracker dict ``_persist_phase_per_profile`` replaces the single ``_current_phase`` for the persistence path — a ``Rotating old backups…`` event tagged for profile B can no longer leak its ``rotator`` tag into profile A's next persisted message. Terminal lines (``Backup complete: …`` etc.) reset the per-profile tracker so the next run's opening messages do not inherit the previous run's last phase.

### Tests
- +4 tests in ``tests/unit/test_run_tab_history_swap.py::TestPersistedPhaseInference``: persisted phase is inferred from the message for the engine-level emits, unmatched messages inherit the previous phase per profile (so ``Backup written: …`` keeps the ``writer`` tag set by the prior ``Copying to Storage…``), the per-profile tracker is reset on terminal messages so a new run starts with a blank phase, and two profiles emitting interleaved LOG events keep their trackers strictly isolated (regression guard against the cross-contamination that a single shared ``_current_phase`` would introduce).

## [3.7.15] - 2026-05-21

### Fixed
- **``safe_remove_tree`` left subtree residuals when an antivirus / indexer held a transient lock past the per-entry budget.** Reproducer in the 18/05/2026 logs: a TestLoic orphan scan logged ``safe_remove_tree left 10 residual(s) under G:\Backup Manager\TestLoic_FULL_2026-05-18_212105`` with ``WinError 145`` (directory not empty) on every nested directory. The per-entry retry totalled ~0.7 s (0.1 + 0.2 + 0.4), short enough that a real-time AV scanner re-checking the freshly-closed backup tree still held handles when the rmdir attempts fired. The bottom-up walk then surfaced the parent dir as "not empty" even though there was nothing visibly inside — the unlinks had failed silently. Fix: ``safe_remove_tree`` now grows an outer-retry layer (``outer_retries`` / ``outer_delay``, default 2 × 1 s exponential) that redoes the whole walk on whatever survived the first pass. The per-entry budget stays at 0.7 s for the fast path, the outer loop pays the AV-release cost only when the first pass actually leaves something behind. Helper ``_safe_remove_tree_pass`` extracted so the loop can re-invoke a clean pass without recursion side effects.
- **Mirror remote upload had no in-phase retry on transient socket drops.** Logs from 15/05/2026: a BLoic mirror SFTP upload failed with ``WriteError: Failed to write tar-stream: Socket is closed``, the scheduler's 35-minute retry fired ``_retry_backup`` and hit the same error identically — the retry happened at the WHOLE-BACKUP layer, not at the mirror layer, so the entire integrity manifest + write phase ran again before the mirror was reattempted. New ``_is_transient_network_error`` helper scans the exception chain (``__cause__`` / ``__context__``) for a curated list of socket-level failure phrases (``socket is closed``, ``connection reset``, ``broken pipe``, ``connection refused``, ``no route to host``, ``ssh session not active``, ``server connection dropped``, ``transport endpoint is not connected``). On a transient match, ``mirror_backup`` now disconnects the dead backend, calls ``get_backend(config)`` again to build a fresh SSH transport, and retries the upload once. Non-transient errors (``ValueError`` on config, etc.) skip the retry — the original behaviour is preserved everywhere else. Default ``_MIRROR_MAX_ATTEMPTS = 2`` (initial + 1 retry); the scheduler-level retry budget still kicks in if both attempts fail, but the common "server kicked the long-lived SSH session" case is now absorbed locally.
- **Scheduler treated the precheck modal timeout as a backup failure.** 18/05/2026 reproducer: the ``destinations unavailable`` modal opened by ``_scheduled_precheck_prompt`` was left unanswered for the full 30-minute timeout. ``_scheduled_backup`` raised a generic ``RuntimeError("Backup cancelled: destinations unavailable")``, which the scheduler's ``_trigger_backup`` classified as ``status=failed``. Two collateral damages — ``crash_recovery_attempts`` was incremented on every overnight drift (three in a row trips the circuit breaker), and the retry budget queued four more pointless re-prompts. New exception ``PrecheckUserTimeoutError`` (in ``src/core/exceptions.py``) carries ``profile_name`` and ``timeout_seconds``. ``_scheduled_precheck_prompt`` now returns ``"timeout"`` (distinct from ``"cancel"``) and ``_scheduled_backup`` raises the new exception accordingly. ``InAppScheduler._trigger_backup`` joins it to the existing ``ProfileLockError`` skip-class — ``status=skipped`` in the journal, no retry, no crash-recovery bump. The constant ``_PRECHECK_PROMPT_TIMEOUT_SECONDS = 30 * 60`` is now module-level so it is the single source of truth shared by the prompt and the exception.
- **Fast-mode verify prompt double-render after a profile-switch + run combination.** Detailed in 3.7.14 — included here only because the test file got a new ``TestPersistentStoreInteraction`` class that pins the regression in two directions.

### Tests
- +3 tests in ``tests/unit/test_safe_remove_tree.py::TestSafeRemoveTreeOuterRetry``: transient ``ENOTEMPTY`` recovers on the second pass, ``outer_retries=0`` opts out cleanly for permanent-failure unit tests, and ``outer_retries=N`` runs exactly ``N + 1`` passes.
- +4 tests in ``tests/test_mirror_failures.py::TestMirrorTransientNetworkRetry``: ``Socket is closed`` retries with a freshly-built backend; non-transient ``ValueError`` skips the retry; two consecutive transient failures bubble up; the dead backend's ``disconnect()`` is called between attempts.
- +5 tests in ``tests/unit/test_scheduler_precheck_timeout.py``: ``PrecheckUserTimeoutError`` is journaled as ``skipped``, never triggers ``_retry_backup`` (even with ``retry_enabled=True``), populates ``detail`` distinct from the concurrent-run case; ``str()`` includes profile name + duration; the exception survives pickling (used in cross-thread propagation).
- +3 tests in ``tests/unit/test_run_tab_header_refresh.py::TestHeaderUpdatedAfterRun``: a late ``_apply_active_backup_type`` reads the refreshed baseline (not the ``load_profile``-time one), ``update_profile_info`` exposes the new ``last_backup`` on the visible label, and the differential auto-promote detection re-fires on refresh — regression guard for the 21/05/2026 captured anomaly even though the current code already respects the contract.

## [3.7.14] - 2026-05-21

### Fixed
- **Fast-mode "Verify now?" prompt rendered twice in the Run-tab log after a backup chain.** User report (21/05/2026): after a sequential ``Start backup`` of L2 → My Backup → TestNP, the Run tab showed the four-row Fast-mode prompt block for TestNP both BEFORE and AFTER the terminal ``Backup complete: 7 files in 2.1 min`` line. Root cause in ``RunTab.clear_log`` (``src/ui/tabs/run_tab.py``): the three cleanup steps ran in the wrong order — ``_clear_log_widget`` emptied ``self._verify_prompts`` (the in-memory dict that ``clear_alerts`` walks to reach the persistent ``VerifyPromptStore``) BEFORE ``clear_alerts``, so the per-card store-clear in ``_destroy_verify_prompt`` had nothing to iterate over and the JSON entry survived. On a subsequent profile-switch back to TestNP, ``_restore_pending_verify_prompt`` replayed the stale entry, the still-in-flight backup pushed its remaining LOG events on top of the restored card, and ``_maybe_prompt_post_backup_verify`` finally stacked a fresh card at the end of the run — two cards around the terminal log line, exactly as on the capture. Fix: ``clear_log`` now calls ``clear_alerts`` FIRST (drains the store via the existing ``_destroy_verify_prompt`` path), THEN ``_clear_log_widget`` (wipes the dict + tree rows). The ``set_current_profile_id`` → ``_reload_log_history`` path is unaffected — it still wipes the dict but leaves the store intact so ``_restore_pending_verify_prompt`` can rehydrate the card on profile switches.

### Tests
- +2 tests in ``tests/unit/test_run_tab_inline_verify_prompt.py::TestPersistentStoreInteraction``: ``clear_log`` purges the ``VerifyPromptStore`` (regression guard for the 21/05/2026 double-prompt), and a ``set_current_profile_id`` profile-switch keeps the store intact so the immediately-following restore can replay it (regression guard against a future "clean both at once" refactor that would silently swallow legitimate pending prompts).

## [3.7.12] - 2026-05-17

### Fixed
- **Run tab showed events from a foreign profile while the user looked at another one.** User report (17/05/2026): a v3.7.10 install was preceded by a manual cancel; on the next launch v3.7.11 was not yet running so the crash-recovery fix did not apply retroactively, and the scheduler crash-recovered the cancelled TestLoic backup. While that backup ran in the background, the user clicked L2 in the sidebar and the Run tab kept moving the progress bar, showing "Copying to Storage…" status, and listing file paths from ``F:\Documents\loicata`` (a TestLoic source, not an L2 source). The Run tab had no way to tell which profile each PROGRESS / LOG / STATUS / PHASE event was about because every event traveled on a single shared EventBus untagged.
- Fix in two parts: **(a)** new ``ProfileTaggingEventBus`` wrapper in ``src/core/events.py`` is installed by ``BackupEngine.run_backup`` for the duration of the run; it ``setdefault``-tags every emit downstream (engine itself, ``PhaseLogger`` instances inside every phase module, individual phase modules) with the active profile id. The wrapper restores the unwrapped bus in the ``finally`` block so a long-lived engine reused across profiles does not bleed the previous profile's id into the next run. **(b)** ``RunTab`` learns its current profile via a new ``set_current_profile_id`` API called from ``BackupManagerApp._load_profile`` on every sidebar switch, and every event handler now drops events whose ``profile_id`` does not match. Untagged events still pass through (back-compat for the Verify tab's own emits, tests).

### Tests
- +10 tests in ``tests/unit/test_profile_tagging_events.py``: ``ProfileTaggingEventBus.emit`` injects ``profile_id``, respects an explicit override, delegates ``subscribe`` / ``unsubscribe`` to the inner bus; the RunTab passes through untagged events (back-compat), passes through matching events, drops foreign events (the 17/05/2026 scenario), accepts anything when no profile is bound yet (cold start), normalises ``None`` to empty string; end-to-end through a real ``EventBus`` — a tagged event reaches the matching profile's tab and a foreign one does not clobber the state; STATUS=success tagged with another profile does not flip the receiving tab's ``_backup_active`` flag (otherwise the next legitimate progress event would be silently dropped).

## [3.7.11] - 2026-05-17

### Fixed
- **User-cancelled backup was treated as a crash on the next app launch.** User report (17/05/2026): cancelled a Fast-mode backup to install v3.7.10; on the next launch ``_check_startup_missed`` auto-fired the backup as crash-recovery. Root cause: ``BackupEngine.run_backup``'s ``except CancelledError`` block ran ``_best_effort_cleanup`` to drop the partial bytes but left ``profile.last_backup_completed=False`` and ``profile.incomplete_backup_name`` populated on the profile JSON. ``_check_startup_missed`` reads exactly those two flags to decide whether to relaunch (``scheduler.py::_check_startup_missed``, ``crash_recovery_due`` branch), and could not tell a clean user-cancel apart from a real crash. Fix: new ``_mark_cancelled`` helper resets the interrupt-recovery flags (``last_backup_completed=True``, ``incomplete_backup_name=""``, ``incomplete_backup_was_full=False``, ``crash_recovery_attempts=0``, restore differential type if forced full) and persists them; the ``except CancelledError`` path now calls it after the bytes have been cleaned up. The crash-recovery circuit breaker counter is also reset because a user-cancel is not a transient failure that should count against the auto-recovery budget — only real crashes should accumulate.

### Tests
- +4 tests in ``tests/test_backup_engine_failures.py::TestCancelClearsCrashRecoveryFlags``: ``last_backup_completed`` is True after cancel, ``incomplete_backup_name`` is empty, ``crash_recovery_attempts`` is reset to 0 (even when pre-seeded to 2 to simulate prior recoveries), and the cleared state is persisted to the profile JSON on disk (re-reading the profile via ``get_all_profiles`` shows the same cleared flags — important because ``_check_startup_missed`` reads from disk, not from the in-memory engine state).

## [3.7.10] - 2026-05-17

### Changed
- **Post-backup "Verify now?" prompt is now an inline card in the Run tab, not a modal Toplevel.** Pre-v3.7.10 the prompt was a ``tk.Toplevel`` with ``transient + grab_set``. The ``grab_set`` confiscated focus from every other window and broke the scheduler-chained workflow: when N profiles finished Fast-mode backups in sequence, N modal Toplevels stacked on top of each other and the user had to dismiss them one by one in order. The new design appends a card to a ``ttk.Frame`` alerts area between the Progress section and the Log treeview in ``RunTab``. N cards coexist vertically, the user acts on each in any order (or ignores them), and further backups keep running with no UI blockage. The card carries the same content as the old dialog — ✓ Backup '<profile_name>' complete header, "verification skipped (Fast mode)" subtitle, "Next periodic in N days" / "No periodic scheduled" status line, **Verify now** / **Dismiss** buttons, and the persistent **Don't ask again for this profile** checkbox. The checkbox commit is now eager-on-toggle (was on-action) so the user opt-out is recorded even if a pending card is left open at app close. ``RunTab.clear_log`` also clears pending cards on profile-switch so a prompt tied to profile A cannot accidentally trigger a verify against profile B.

### Tests
- +9 tests in ``tests/unit/test_run_tab_inline_verify_prompt.py``: alerts area starts empty, prompt appends a card, three sequential prompts coexist (the user-reported regression scenario), card is parented to the alerts area, Verify-now / Dismiss buttons invoke their callback AND destroy the card, Don't-ask checkbox toggle fires the callback with the new state, ``clear_alerts`` and ``clear_log`` both drop pending cards.

## [3.7.9] - 2026-05-17

### Fixed
- **Profile-switch click still froze 3-5 s after the v3.7.8 install.** v3.7.8 fixed only one of two identical ``rglob("*.wbenc")`` calls in ``RecoveryTab``: the sister copy survived inside ``_on_backup_path_changed`` (recovery_tab.py:1409) — a copy-paste twin of the fixed branch that was never refactored into a shared helper. The trace ``load_profile → _fill_fields → backup_path_var.set`` fires this twin on every profile switch, walking the entire USB destination just like the v3.7.8-targeted call did. Fix: same shallow ``glob("*.wbenc")``. ``{backup_name}.tar.wbenc`` always sits at the storage root (``backup_engine.py::_phase_write``); a non-recursive glob covers the legitimate case.

### Tests
- Extended ``tests/unit/test_recovery_tab_no_recursive_glob.py`` to cover ``_on_backup_path_changed`` in addition to ``_update_post_source_sections``. Both call sites are pinned by AST-level inspection; a future refactor that reintroduces ``rglob`` in either branch trips the regression immediately. The duplication is left in place (small enough that consolidation would be cosmetic), but the test guards against it.

## [3.7.8] - 2026-05-17

### Fixed
- **Profile-switch click took 2.9-6.6 s before the tabs repainted.** Root cause located by the v3.7.7 instrumentation (``[LP-PROFILE]`` timings in ``backup_manager.log``): ``RecoveryTab._update_post_source_sections`` ran ``src.rglob("*.wbenc")`` on the storage destination path. The path typically points at a USB HDD root holding 268 k+ files across all profiles' backups, and ``rglob`` walked the entire tree to ask a question that only needs the storage root: "is there a ``{backup_name}.tar.wbenc`` next to my backup dirs?". The call was triggered on every profile switch via ``load_profile → source_type_var.set → _on_source_type_changed → _update_post_source_sections``. Fix: ``rglob`` → ``glob``. Encrypted backups are always written as ``{backup_name}.tar.wbenc`` at the storage root (see ``local_writer.py::write_encrypted_tar``, ``backup_engine.py::_phase_write``), never nested inside a backup directory — a shallow glob is functionally equivalent and runs in microseconds. The 17/05/2026 case dropped from 2.9-6.6 s per switch to near-instant.

### Removed
- v3.7.7's temporary instrumentation (``[LP-PROFILE]`` / ``[LP-HEALTH]`` timing logs in ``_load_profile`` and ``_update_health_dashboard``). Its job is done: the recovery-tab ``rglob`` was the single cost (99.8 % of the wall-clock); everything else timed at 0-4 ms. The contract is now pinned by a regression test rather than left as live instrumentation.

### Tests
- +1 test in ``tests/unit/test_recovery_tab_no_recursive_glob.py``: AST-level inspection of ``_update_post_source_sections`` source — rejects any reintroduction of ``.rglob(`` and pins the presence of the shallow ``.glob("*.wbenc")``. Comments mentioning the regression by name are ignored via an AST round-trip so the docstring/rationale can keep explaining the original bug.

## [3.7.7] - 2026-05-17

### Added (diagnostic build, temporary)
- **``_load_profile`` and ``_update_health_dashboard`` are instrumented with per-step ``time.monotonic`` timing**, logged at INFO level with the ``[LP-PROFILE]`` and ``[LP-HEALTH]`` prefixes in ``backup_manager.log``. Each tab.load_profile, the retention/protection swap, the Run-tab refresh, the journal lookup, the destination-validate fan-out, and the dashboard card updates are timed independently. Reported on 17/05/2026: ~6 s freeze on every profile-switch click — before fixing we want to know whether the cost concentrates in one tab, the health dashboard, or fans out evenly. This instrumentation will be removed in v3.7.8 once the dominant cost is pinned and replaced by a regression test on the fix.

## [3.7.6] - 2026-05-17

### Fixed
- **Post-wizard ~10 s white-window freeze after creating a 2nd profile.** Root cause: ``_load_profiles`` always ended with an implicit ``_load_profile(first_active_profile)`` — the 11-tab fan-out + health-dashboard refresh on the first active profile in the sidebar. ``_new_profile``, ``_move_profile_up``, and ``_move_profile_down`` then *also* called ``_load_profile`` (directly or via ``_reselect_profile``) on their actually-targeted profile (the new one / the moved one). Net effect: every "create a new profile" or "reorder profile" action ran the full per-profile load TWICE — once on first_active, once on the target — doubling the freeze window. The newly-deiconified main window stayed unpainted (white) until the second load returned.

### Changed
- ``_load_profiles`` now accepts ``select_first: bool = True`` (keyword-only). The three callers that own the post-reload selection (``_new_profile``, ``_move_profile_up``, ``_move_profile_down``) pass ``False``; the implicit ``_load_profile(first_active)`` is skipped and the targeted profile is loaded ONCE. Startup paths and ``_save_profile`` / ``_relaunch_wizard_after_delete`` keep the default ``True`` because they rely on the implicit load to populate the tabs after the sidebar refresh.

### Tests
- +8 tests in ``tests/unit/test_app_load_profiles_select_first.py``: signature is keyword-only with default True; the three optimized callers pass ``select_first=False``; the two preserve-default callers do NOT pass False (so they keep showing tab content after Save / wizard-relaunch-after-delete); behavioural pair — ``select_first=False`` skips the implicit ``_load_profile`` while ``select_first=True`` invokes it on the first active profile.

## [3.7.5] - 2026-05-17

### Fixed
- **Creating a second profile via "New profile" auto-fired an unwanted backup of the brand-new profile** while the UI was still refreshing. Root cause: ``BackupManagerApp._new_profile`` called ``scheduler.mark_triggered_now`` AFTER ``self._load_profiles()`` — but ``_load_profiles`` is synchronous on the main Tk thread and takes 5-10 s on a populated config (28 KB schedule journal + 11-tab refresh). During that window the scheduler daemon (CHECK_INTERVAL = 30 s) ticks, sees the new profile with ``last_trigger is None`` in ``_state``, and ``_is_due`` returns True on its first branch — firing an unwanted backup of the profile the user had not yet had a chance to review. ``mark_triggered_now`` finally arrived too late. Fix: a new helper ``_seed_scheduler_for_new_profile`` arms BOTH the backup and the periodic-verify clocks on the new profile and is invoked IMMEDIATELY after ``save_profile``, BEFORE ``_load_profiles``. The race window collapses from ~10 s to the microseconds between two consecutive method calls.
- **``mark_verify_now`` was not called at all on the ``_new_profile`` / ``_relaunch_wizard_after_delete`` paths.** The v3.7.4 fix seeded the periodic-verify timer only for profiles created via the first-launch wizard; second-and-later profiles went back to the v3.7.3 behaviour (immediate periodic verify on the first scheduler tick). The new helper now seeds both timers on every creation path, including the wizard-relaunch-after-delete branch.
- **First-launch wizard post-save block consolidated** to use the same helper, removing the duplicated local-import-of-datetime + two-call dance.

### Tests
- +4 tests in ``tests/unit/test_app_new_profile_seeding.py``: the helper calls both ``mark_triggered_now`` and ``mark_verify_now`` with the same profile id and the same timestamp; ``inspect``-based static checks pin that ``_seed_scheduler_for_new_profile`` precedes ``_load_profiles`` in both ``_new_profile`` and ``_relaunch_wizard_after_delete``, so a future refactor cannot silently re-introduce the race.

## [3.7.4] - 2026-05-17

### Fixed
- **Periodic integrity verification fired on the first scheduler tick after a profile was created.** Root cause: ``InAppScheduler._check_verify_due`` treated ``last_verify is None`` as "verification due right now" instead of "timer not yet seeded". On a profile created at ``T``, the scheduler thread started, fired ``_check_schedules`` ~30 s later (``CHECK_INTERVAL``), entered ``_check_verify_due`` with ``last_verify=None``, and immediately triggered ``IntegrityVerifier.verify_all()`` — even though the user-configured ``verify_interval_days`` was 7. Visible on 2026-05-17: profile ``TestLoic`` saved at 15:47:51 had its first periodic verify recorded at 15:48:47, 56 s after creation. Fix: ``_check_verify_due`` now seeds ``last_verify = now`` on the first observation and returns; the first real periodic verify is due ``interval_days`` after creation, not on the next tick.
- **Periodic verify re-hashed backups belonging to OTHER profiles** that shared the destination directory. ``IntegrityVerifier.verify_iter`` called ``backend.list_backups()`` without filtering, while the rotator already filtered by ``sanitize_profile_name(profile_name) + "_"``. Concretely on the 2026-05-17 case: the newly-created ``TestLoic`` profile's accidental first-tick verify re-hashed 39 873 + 3 339 files from two unrelated ``TestBackup*`` profiles in parallel with its own first backup hash phase, wasting USB I/O bandwidth and CPU. ``verify_iter`` now applies the same profile prefix as the rotator — only the caller's own backups are in scope.

### Added
- **``InAppScheduler.mark_verify_now(profile_id, dt=None)``** — public API symmetric to ``mark_triggered_now``. Out-of-band callers (wizard, profile import, manual verify) use it to seed the periodic-verify clock so the next tick does not fire immediately on a freshly-created profile. The wizard's post-save block in ``src/ui/app.py`` now calls both ``mark_triggered_now`` and ``mark_verify_now`` on every profile it produces, replacing the previous direct poke of the private ``scheduler._state.set_last_trigger``.

### Tests
- +6 tests in ``tests/unit/test_scheduler_periodic_verify_first_launch.py``: first observation seeds the timer without instantiating ``IntegrityVerifier``; second call within the interval is silent; call past the interval does trigger; ``mark_verify_now`` records state, defaults to ``datetime.now()``, and prevents the immediate first-tick trigger when called from a wizard-style flow.
- +4 tests in ``tests/unit/test_integrity_verifier_profile_filter.py``: foreign-profile backups are skipped on a shared destination (the 2026-05-17 ``TestLoic`` / ``TestBackup*`` scenario); the prefix is anchored at ``_`` so ``Foo`` does not match ``FooBar_FULL_…``; ``sanitize_profile_name`` is applied so ``My Profile`` matches ``My_Profile_FULL_…``; a destination holding only foreign backups yields zero results without errors.
- Existing IntegrityVerifier suites updated to pass ``name="Backup"`` / ``name="Test"`` on the ``BackupProfile`` fixtures so the new profile filter matches the historical backup names (``Backup_FULL_…``, ``Test_FULL_…``). ``test_backup_sidecar_filtering`` pins ``profile.name`` on its ``MagicMock`` to avoid a ``sanitize_profile_name(MagicMock)`` ``ValueError``.

## [3.7.3] - 2026-05-17

### Fixed
- **MSI MajorUpgrade silently removed the Defender exclusion on the install folder, causing 5-10 min Defender-scan freezes on every first launch after upgrade since v3.7.0.** Root cause: the WiX ``RemoveDefenderExclusion`` CustomAction was conditioned only on ``REMOVE="ALL"``, which is set during a MajorUpgrade's ``RemoveExistingProducts`` step (silent uninstall of the previous version to make room for the new one). The old version's exclusion was therefore stripped, and the new version's ``AddDefenderExclusion`` either skipped (timing race on the ``Installed`` property) or was silently blocked by Windows 10/11 **Tamper Protection** (introduced in 1903, blocks third-party processes from modifying Defender preferences even from elevated MSI context). Net effect: the install folder ended un-excluded, and Windows Defender real-time scanned every one of the ~800 embedded data files of the Nuitka binary on every module load at startup. The fix adds ``NOT UPGRADINGPRODUCTCODE`` to both ``RemoveDefenderExclusion`` and its paired ``SetRemoveDefenderCmd`` conditions so the Remove CA fires only on a genuine user-initiated uninstall, never during the upgrade-driven silent uninstall. The exclusion is now preserved across upgrades.
- **General tab "Integrity verification" section had a visible gap between the two checkboxes** on local-plain profiles. The hint label below "Verify integrity after backup" was always packed (reserving one line of vertical space) even when its text was empty — only populated when storage is remote or Object Lock forces verify on. The hint is now packed dynamically: shown only when there is a message, ``pack_forget()`` otherwise.

### Tests
- +1 test in ``tests/test_build_msi.py::TestDefenderExclusion::test_remove_exclusion_skips_on_upgrade``: pins ``NOT UPGRADINGPRODUCTCODE`` on both ``SetRemoveDefenderCmd`` and ``RemoveDefenderExclusion`` conditions so a future refactor of the WiX template can't silently regress this property.

## [3.7.2] - 2026-05-17

### Fixed
- **Run-tab Destinations card flashed "Connection test timed out after 30s" right after a backup completed**, then recovered to "X GB free" on the next health poll 60 s later. Visible since v3.7.1 because the pool4 perf win shortened backup duration, leaving the USB HDD idle earlier and hitting its spin-down threshold sooner; the first post-backup health probe lands while the drive is still spinning up, which exceeds ``CONNECTION_TIMEOUT = 30 s`` on deep-power-save HDDs. The fix lives in ``src/core/health_checker.py::_check_destination``: after a failed ``test_connection()``, the message is matched against a transient-marker list (``"timed out"``, ``"drive not ready"``). On a match, ``test_connection()`` is called once more — by then the drive is responsive and the probe succeeds. Non-transient errors (permission denied, connection refused, missing destination) skip the retry and surface immediately. Behaviour change is purely cosmetic; the backup pipeline does not use this code path.

### Tests
- +10 in ``tests/unit/test_health_checker.py::TestTransientWakeupRetry``: marker matching (``timed out``, ``drive not ready``, case-insensitive), rejection of non-transient strings (``permission denied``, ``connection refused``, empty), retry-on-transient that succeeds on the 2nd probe, retry-on-transient that fails twice and surfaces the second message, no-retry paths on real failures and on success.

## [3.7.1] - 2026-05-17

### Changed
- **``write_flat`` now drives ``shutil.copy2`` from a ``ThreadPoolExecutor`` of 4 workers.** Byte transfer still goes through ``CopyFileExW`` (Windows kernel zero-copy, Invariant 1 of ``invariants_perf_critical.md``) — only the per-file driving loop is parallel. On the 2026-05-17 bench (``scripts/bench_copy_strategies.py``, 7.38 GB / 3 642 files / HDD USB external) pool4 gained **+39 %** vs single-thread (28.0 s → 20.1 s, 270 → 375 MB/s) while pool8 only added +5 % over pool4 — 4 is the empirical sweet spot for an HDD spindle. Projected gain on the 47 GB / 271 k-file BLoic workload: backup wall-clock 55 min → ~40 min, since per-file overhead dominates more on small-file workloads than on the 2-MB/file Divers proxy.
- **``PhaseLogger.progress()`` is now thread-safe.** A ``threading.Lock`` guards the throttle window (``_last_progress_ms`` read/update) so two parallel writer workers cannot double-emit within the same 100 ms window and defeat Invariant 5 (PROGRESS at most 10 Hz to keep the Tk message pump alive). Uncontended on the sequential phases (collector, filter, hashing-internal loop) and adds ~50 ns there.

### Added
- **``scripts/bench_copy_strategies.py``** — committed alongside the result JSON ``bench_results_20260517_114129.json`` so the empirical basis for ``WRITE_FLAT_WORKERS = 4`` is reproducible. Bench compares single / pool4 / pool8 modes with OS cache eviction between runs (20 GB dummy write) to keep the comparison cold-cache for every mode.

### Tests
- +7 tests in ``tests/test_local_writer.py::TestWriteFlatParallel``: ``WRITE_FLAT_WORKERS == 4`` is pinned; pool is constructed with ``max_workers=4``; 50 files all land regardless of completion order; ``cancel_check`` propagates from any worker; first ``WriteError`` surfaces; PROGRESS still emits once per file with throttle disabled; ``long_path_mkdir`` race on deeply-nested concurrent directories is safe.
- Updated ``tests/test_write_error_failfast.py::test_first_file_failure_stops_pipeline``: the pre-v3.7.1 strict bound ``mock_copy.call_count == 1`` (only the first file's copy attempted before the loop broke) is replaced by ``≤ WRITE_FLAT_WORKERS``. A cross-worker short-circuit on the first observed error (``first_error[]`` shared state, ``error_lock``) pins this stricter form deterministically — without it, worker rotation races could burn 5-10 extra copy attempts on a tmp_path NVMe before the main thread observes the first exception.

## [3.7.0] - 2026-05-17

### Added
- **Per-profile "Verify integrity after backup" toggle (Fast / Thorough mode).** Before 3.7.0 every successful backup re-hashed every file right after the copy phase to confirm the destination bytes matched the manifest — on the 47 GB / 271 k-file BLoic profile this added ~19 min to a 70-min run on a 5400 rpm HDD. The new toggle (defaults to **Fast = OFF** for new profiles) lets the user trade post-copy verify wall-clock for a periodic verification that runs out-of-band on the schedule the user already picked. Local plain and local encrypted (.tar.wbenc) profiles honour the toggle directly; **SFTP, S3, and Object Lock force-on regardless** (verify cost is dwarfed by upload cost on remote, and Object Lock anti-ransomware requires integrity by contract). The engine helper ``_effective_auto_verify(profile)`` resolves the user toggle against these overrides and is the single source of truth for the ``_phase_verify`` early-exit.
- **Wizard step 6 — "How thorough should the backup be?"** Personal mode goes from 5 to 6 steps. The new final step (Fast / Thorough radio) maps directly to the new toggle. Pro mode keeps its 11 steps unchanged because Object Lock anti-ransomware forces verify on.
- **Integrity verification section moved from the Schedule tab to the General tab** (between Source paths and Exclusion patterns). The new "Verify integrity after backup" checkbox sits above the existing "Enable periodic integrity verification" / "Verification interval" pair, so the user sees both options in the same place — the post-copy verify and the periodic safety net.
- **Post-backup "Verify now?" dialog for manual Fast-mode backups.** After a manual backup completes successfully and the post-copy verify was skipped, a modal Toplevel offers a one-click ``Verify now`` (switches to the Verify tab and triggers the verify on the current profile) plus a ``Skip`` button and a ``Don't ask again for this profile`` checkbox (persisted to ``BackupProfile.dont_prompt_verify_after_skip``). The dialog is suppressed when verify ran inline (remote / Object Lock / user toggle on), when the user opted out previously, and on scheduled runs (no one is in front of the screen — the email notifier handles those instead).
- **Email "verification disabled" tag for scheduled Fast + no-periodic backups.** When a scheduled backup completes successfully in Fast mode AND no periodic verification is armed for the profile (v3.7.0 case 3), the subject is prefixed with ``⚠️`` and suffixed with ``, verification disabled`` so the warning is visible in inbox previews, and an amber warning block is inserted in the HTML body (variant A wording) telling the operator that no automatic integrity check will confirm this backup and pointing to the Verify tab.

### Changed
- **``VerificationConfig.auto_verify`` default flips from True to False.** New profiles ship Fast-mode by default; existing profiles already on disk are not migrated (the user manually validated on 2026-05-17 that no in-the-wild profiles need migration).
- **Wizard ``_create_profile`` reads ``verify_after_backup`` from the new step 6** and constructs a ``VerificationConfig`` accordingly. Pro-mode flow does not consume the key (Object Lock force-on overrides it).

### Tests
- +16 tests in ``tests/test_skip_verify_after_backup.py``: ``VerificationConfig`` default-off contract, ``_effective_auto_verify`` resolution across LOCAL plain, LOCAL encrypted, SFTP, S3, NETWORK and Object Lock, and engine ``_phase_verify`` integration (skip when user off+local, run when user off+remote, run when user off+Object Lock, run when user on+local). The engine tests build a minimal ``BackupEngine`` via ``__new__`` + ``MagicMock`` ``_events`` so the verify-dispatch path is testable without touching the full pipeline.
- +13 tests in ``tests/test_email_verification_disabled.py``: subject tag fires only on ``success`` (never on FAILED / CANCELLED), amber HTML block present/absent across both ``_build_html`` and ``_build_backup_html``, and the kwarg defaults to False so v3.6.x callers keep their previous output byte-for-byte.
- Updated ``tests/unit/test_wizard_schedule_step.py``: renamed ``test_personal_mode_has_five_steps`` → ``test_personal_mode_has_six_steps`` and updated ``test_next_says_finish_on_last_step`` to anchor on step 6 (the new Backup-speed final step).

## [3.6.7] - 2026-05-15

### Fixed
- **SFTP mirror upload failed at +1 s with `WriteError: Failed to write tar-stream: Socket is closed`.** Root cause: ``assets/server_helper.sh`` shipped with **CRLF line endings**. ``core.autocrlf=true`` on Windows had rewritten the file at checkout, the PyInstaller / Nuitka bundle embedded the corrupted version, and on the Pi the Linux kernel read the shebang as ``#!/bin/bash\r``, failed to find that interpreter, and closed the SSH channel before any tar byte could land. Fix in three layers: (a) ``.gitattributes`` now pins ``*.sh text eol=lf`` so a future checkout cannot recreate the problem; (b) ``_get_helper_bytes_and_hash`` strips ``\r\n`` defensively before computing the deploy hash and pushing the bytes, so a stray CRLF anywhere in the toolchain still produces a working helper; (c) ``_has_gnu_tar`` and ``_remote_file_hash_matches`` now cap their ``recv`` loops at 64 KB / 4 KB and bail on non-bytes chunks — without this guard a misbehaving channel (mock or otherwise) accumulates an unbounded loop and on the 2026-05-15 test run consumed >2.25 GB of private bytes before Windows started thrashing the pagefile.
- **Run-tab Log lost the final ``Backup complete: N files in X min`` row.** The engine emits ``STATUS=success`` immediately before the terminal LOG, and on Windows Tk can process ``_update_status`` (flipping ``_backup_active`` to False) before the LOG's ``after(0, _append_log)`` is even scheduled — silently dropping the only row that carries the run duration. ``_on_log`` now always lets terminal lines (``Backup (complete|failed|cancelled)`` matched via ``_TERMINAL_LOG_PATTERN``) through, regardless of the gate. The cross-tab pollution that originally motivated the gate is unaffected because the Verify tab never emits those messages.
- **Run-tab status label stayed frozen on ``Measuring bandwidth (Mirror N)...`` for the entire mirror upload (43 min on the 260 k-file BLoic run).** ``apply_throttle`` emits a PHASE_CHANGED for the bandwidth probe; nothing in ``mirror_backup`` re-announced when the upload itself began, so the label and the log feed desynced. ``mirror_backup`` now emits ``PHASE_CHANGED("Uploading to Mirror N...")`` immediately after ``apply_throttle`` returns.

### Added
- **Run-tab "Last backup" card now shows ``Source size``** next to the file count, matching the same line in the success email. Persisted in ``ScheduleLogEntry.bytes_source`` so the card survives an app restart. Older journal entries from before 3.6.7 default the field to 0 and the line is suppressed rather than showing ``0 B``.
- **``scripts/run-bounded.ps1``** — a PowerShell wrapper that monitors a subprocess tree's private commit and force-kills it when a configurable cap is exceeded (default 2 GB). Recursive BFS over child PIDs via a single ``Win32_Process`` snapshot, with parent-StartTime sanity check to drop recycled PIDs. Background: two desktop freezes from python.exe accruing 100+ GB virtual memory and saturating the pagefile (no BSOD, just unresponsive). Use it on every long-running Python invocation until the underlying leak is gone. Validated end-to-end: Nuitka build peaks 3.4 GB / cap 6 GB; ``pytest tests/test_sftp_tar_upload.py`` (which was OOMing at >2.25 GB before the recv-loop fix) now peaks at 34 MB.

### Tests
- +4 tests in ``tests/test_sftp_helper_deployment.py``: static LF check on the shipped asset, CRLF→LF defensive normalisation, and two unbounded-recv OOM guards (one per probe path).
- +2 tests in ``tests/test_run_tab_progress_isolation.py``: terminal LOG passes through even after ``STATUS=success``; same for ``Backup failed: …``.
- +1 test in ``tests/test_mirror_failures.py``: ``mirror_backup`` re-emits PHASE_CHANGED after ``apply_throttle`` so the Run-tab label resyncs with the actual work.
- +2 tests in ``tests/test_scheduler.py``: ``bytes_source`` round-trips through ``ScheduleLogEntry`` and defaults to 0 for older entries.

## [3.5.9] - 2026-05-13

### Changed
- **Run-tab Log "Backup complete" line is shorter and reads in minutes.** The final summary used to read ``Backup complete: 231908 files in 7831.8s → Storage (Start SSH server cipango56@192.168.2.149:22)``. The ``→ Storage (…)`` tail repeated information already visible on the Storage tab and in the History row; on narrow windows it pushed the file count + duration off-screen. The duration in seconds was also hard to read at a glance — ``7831.8s`` requires a mental division to mean anything, whereas ``130.5 min`` is immediately parseable. The new shape is ``Backup complete: 231908 files in 130.5 min``. The History tab's status classifier (``_extract_status``) still keys on the substring ``"Backup complete:"`` so the format change is backward-compatible with old log files.
- **Run-tab Log Phase column is now blank on the terminal "Backup complete / failed / cancelled" row.** Previously this row inherited the last seen phase tag (``rotator`` on the success path, an earlier one on a mid-pipeline failure) because the engine emits the line through ``_log()`` without a fresh ``PHASE_CHANGED`` and ``RunTab._on_log`` falls back to ``_current_phase``. The terminal messages now match a dedicated regex (``^Backup (complete|failed|cancelled)``) that explicitly clears the column — semantically correct (nothing is running) and prevents the stale tag from leaking into a follow-up run via ``_current_phase``.

### Tests
- +5 tests in ``tests/unit/test_backup_complete_summary.py`` pin both contracts: the minutes-format conversion (``7831.8s → "130.5 min"``), the absence of ``→`` and ``Storage (`` in the summary, and the terminal-message detection across 5 positive and 7 negative cases. The ``test_terminal_messages_have_no_inferred_phase`` guard ensures a future ``_PHASE_PATTERNS`` entry that accidentally matches ``Backup …`` won't restore the stale-tag behaviour.
- ``tests/test_build_msi.py::TestDefenderExclusion`` updated to slice on ``SetAddDefenderCmd`` / ``SetRemoveDefenderCmd`` instead of the old executor IDs — the 3.5.8 ``WixQuietExec64`` migration moved the PowerShell ``-ErrorAction SilentlyContinue`` from the executor CA into a paired setter CA.

## [3.5.8] - 2026-05-13

### Fixed
- **Verify-mirror race condition false-positives the whole backup on volatile source files.** ``_verify_mirror_checksums`` and ``_verify_remote_checksums`` re-hashed ``f.source_path`` at verify time and compared the fresh digest to the remote checksum. Any source file that mutated between the manifest phase (T+0) and the verify phase (T+~1 h on a 262 k-file workload) produced a guaranteed ``Hash mismatch on Mirror 1`` and aborted the run. Reproducer from the 2026-05-12 incident: a profile that includes ``.claude/`` while Claude Code is open — ``settings.local.json`` is rewritten on every permission grant — failed every overnight backup with exactly that single file in the error list. The fix replaces the live re-hash by a lookup in ``ctx.integrity_manifest["files"][rel_path]["hash"]``: the manifest is the canonical description of the backup contents and the live source is irrelevant once collection has run. Semantics are now ``H_manifest == H_remote`` (the mirror reflects what was backed up) instead of ``H_source_now == H_remote`` (the mirror reflects whatever the source happens to be at this moment, which is not a property anyone wants to verify). The local-USB verify path is unchanged — it already compares destination bytes to the manifest.
- **Side effect: the mirror-verify phase now runs in seconds instead of ~40 min on large workloads.** Once the verify is pure dict lookup in memory, the sequential per-file loop is no longer I/O-bound and the previous ``compute_sha256(f.source_path)`` for each of 262 691 files (single-threaded, while the manifest phase used 8 workers) disappears. On the 2026-05-13 BaLoic run this represents ~37 min recovered per backup.
- **``with_retry`` no longer retries on ``FileNotFoundError``.** ``delete_backup`` raises ``FileNotFoundError("Backup not found: ...")`` when the artefact is already gone — a terminal state, not a transient failure. The retry decorator caught it in the broad ``except Exception`` and waited through two exponential-backoff cycles (1.6 s + 2.4 s) before giving up, flooding the log with two ``[WARNING] storage.base: delete_backup attempt N/3 failed`` lines per absent artefact. ``_best_effort_cleanup`` already swallows FNF downstream and treats it as success, so propagating immediately is both correct and silent. The case fires on every ``_best_effort_cleanup`` of an unencrypted profile because the cleanup probes both ``X`` and ``X.tar.wbenc`` to cover the encryption-flag-mid-flight edge case.
- **``.claude/`` excluded by default for new profiles.** The directory holds Claude Code's per-project state — ``settings.local.json`` is rewritten constantly, ``.claude/projects/<hash>/memory/*`` carries machine-local memories — and has no backup value. Existing profiles are not migrated; the user can add the pattern manually in the General tab if they want the new behaviour.
- **Save button vanished from every tab after the first round-trip through Run / History / Recovery / Verify.** ``_on_tab_changed`` re-packed ``_save_frame`` with ``pack(fill="x", side="bottom")`` after the no-save tabs called ``pack_forget()``, but the constructor's ``before=self.notebook`` argument was not preserved on the re-pack. Tk's pack manager appended the frame at the END of the pack list, behind the notebook that was packed at startup with ``expand=True``. The notebook then claimed the entire vertical cavity and ``_save_frame`` was allocated zero height — invisible at the bottom of every saveable tab, with no way for the user to commit profile changes (the user actually had to give up editing Mirror 1 credentials because Save was unreachable). The fix re-adds ``before=self.notebook`` to the four call sites that re-pack the frame (``_on_tab_changed``, About close, Bug Report close, Ready dialog close), and mirrors the contract on the notebook re-packs (``before=self._save_frame``) so the bug cannot resurface if the order of operations ever drifts.
- **MSI installer flashed a console window during Defender-exclusion CustomAction.** The 3.5.1 attempt at fixing this (``-WindowStyle Hidden`` on a plain ``ExeCommand`` CustomAction) was insufficient on Windows 10/11: the console host (``conhost.exe``) is spawned by the OS BEFORE PowerShell processes ``-WindowStyle Hidden``, producing a ~150 ms black rectangle that users perceived as the installer crashing. The CustomActions now invoke ``WixQuietExec64`` from ``WixUtilExtension``, which uses ``CreateProcessEx`` with the ``CREATE_NO_WINDOW`` flag — no conhost is ever created and no window can flash. Deferred CAs cannot read installer properties directly, so a paired immediate "setter" CA (``SetAddDefenderCmd`` / ``SetRemoveDefenderCmd``) populates ``WixQuietExec64CmdLine`` with the resolved ``[INSTALLFOLDER]`` path before the deferred executor reads it. ``Return="ignore"`` keeps a disabled / blocked Defender from breaking the install. The same flicker is also gone on uninstall.

### Tests
- +7 tests in ``tests/test_verify_uses_manifest_hash.py`` cover the regression on both verify paths: source mutated after the manifest closes still passes verify, real remote-checksum drift still raises, missing manifest entry falls back to size compare, and missing remote file still raises. The two ``source_modified_after_manifest_does_not_fail_verify`` tests fail on 3.5.7 with the exact ``Hash mismatch`` that produced the 2026-05-12 incident.
- +1 test in ``tests/test_base_storage.py::TestWithRetry::test_file_not_found_error_propagates_without_retry`` — surfaces in under 1 s and pins the no-retry contract; would otherwise wait through the full backoff window.
- +1 test in ``tests/test_config.py::TestBackupProfile::test_default_excludes_dot_claude`` — anchors ``.claude`` in the default exclude list so a refactor of ``BackupProfile`` defaults cannot silently drop it.
- +3 tests in ``tests/unit/test_save_button_visibility.py``: initial pack order, single Run → General switch (the exact 2026-05-13 reproducer), and a ten-cycle stress loop. All three pin the invariant ``pack_slaves().index(_save_frame) < pack_slaves().index(notebook)`` after every re-pack — a pure pack-order assertion that does not depend on the window being realised, so it runs deterministically under the session-scoped withdrawn Tk root.

## [3.5.5] - 2026-05-11

### Fixed
- **Retention tab "Days of history" row stayed hidden after a Monthly → Daily schedule switch.** When the user picked Monthly in the Schedule combobox and saved, ``weekly_row`` was ``pack_forget()``-ten. A subsequent switch to Daily called ``daily_row.pack(before=weekly_row)`` before re-packing weekly_row, which raised ``_tkinter.TclError: window isn't packed``. The exception was silently swallowed by the ``StringVar`` trace callback (Tkinter prints it to stderr but does not propagate), so the Daily row never reappeared in the UI even though the saved profile was correctly set to Daily. Monthly → Weekly was unaffected because weekly_row uses ``before=monthly_row`` and monthly_row is never ``pack_forget()``-ten. The fix re-orders the two ``pack``/``pack_forget`` blocks so weekly_row is re-packed *before* daily_row tries to anchor itself to it.

### Tests
- +7 tests in ``tests/unit/test_retention_frequency_sync.py`` cover every Schedule → Retention frequency transition (3 initial states + Monthly → Daily, Monthly → Weekly, full Save+reload after Monthly → Daily, and a sequence-of-transitions guard that asserts ``_apply_frequency_visibility`` never raises). The Monthly → Daily and Save+reload tests fail on 3.5.4 with the exact ``TclError: window isn't packed`` that produced the original silent bug.

## [3.5.4] - 2026-05-10

### Changed
- **Run-tab Log restored to a proper two-column layout (``Message`` / ``Phase``)** — the 3.5.0 build had drifted to a single-column ``show="tree"`` rendering that prefixed messages with ``[phase]`` text, losing the visual alignment between phase identifiers and message bodies. The widget now uses ``show="tree headings"`` with ``columns=("phase",)``: column ``#0`` is the tree column carrying the message text, the caret and the native indentation; the ``Phase`` column is fixed-width (90 px) on the right and shows the phase name only on top-level event rows. Child rows (categories, extensions, paths under Skipped, patterns under Applying exclude patterns) leave the ``Phase`` cell empty — a leaf path has no phase of its own. The caret stays glued to the message it expands, which the previous one-column hack achieved through string concatenation.

## [3.5.3] - 2026-05-10

### Changed
- **Run-tab status label stays grey throughout the run.** ``_update_phase`` used to switch the foreground to ``Colors.ACCENT`` (blue) on every PHASE_CHANGED event, painting "hashing: …", "copying: …", "verifying: …" in blue while only the heartbeat "Scanning… N files in M folders" was grey. The result was a visual inconsistency between phases — the user perceived blue as a state highlight, which competed with the canonical green ``Backup complete!`` / red ``Backup failed!`` end-of-run signals. The label now renders ``Colors.TEXT_SECONDARY`` (grey) for every in-flight phase; only the terminal states keep their colour (green = success, red = failure).
- **Default ``exclude_patterns`` no longer hides ``*/evidence/*/volatile``.** The path-style pattern was added in 3.3.20 to suppress the permission-denied flood from WardSOAR volatile-memory evidence stores, but cybersecurity evidence is exactly the kind of data the user *wants* to back up — silently filtering it was the wrong default. New profiles created from 3.5.3 onwards have 9 default patterns instead of 10. Existing profiles are not migrated; the user can remove the pattern manually from the General tab if they want the new behaviour. The pattern itself still works when explicitly added to a profile's exclude list.

## [3.5.2] - 2026-05-10

### Fixed
- **Collector phase silent for ~60 s on large workloads — looked like the app had crashed.** On a 262 654-file profile the recursive walk between ``Applying exclude patterns (10)`` and ``Collected N files from M sources`` ran for a full minute without emitting any event, leaving the Run-tab Log frozen and the progress bar at 0 %. The user had no visible feedback that the app was still working.
  - New ``_ScanHeartbeat`` class in ``collector.py`` ticks at every scanned file/directory and emits a ``PROGRESS`` event with ``total=0`` (signalling "indeterminate scan" rather than a percentage). ``PhaseLogger.progress`` already throttles to ~10 Hz so the bus only sees a few hundred events even on a 100 k-entry walk — no flooding.
  - The Run-tab's ``_update_progress`` now interprets ``total == 0`` as a scan heartbeat and updates the status label (``Scanning... 47823 files in 1234 folders``) without touching the determinate progress bar. The bar stays at 0 % until manifest / write / verify report real ratios, which is the expected behaviour for those phases.
  - Net effect: during a long collect, the user sees the file/folder counts climb in real time. Visible proof of life replaces the apparent freeze.

## [3.5.1] - 2026-05-10

### Fixed
- **``OverflowError: timeout value is too large`` crashed every backup with more than ~143 000 files.** The hash-phase deadline introduced in 3.4.0 (``max(60 s, N × 30 s)``) overflowed Windows' DWORD-millisecond wait limit (``WaitForMultipleObjectsEx``, ~49.7 days) once the file count exceeded 143 k — Python refused to schedule the wait and the engine surfaced ``Backup failed: timeout value is too large`` in the Run-tab Log right after ``Building integrity manifest...``. The fix adds an absolute ceiling ``_HASH_TIMEOUT_MAX_SECONDS = 4 h`` (and a mirror ``_VERIFY_TIMEOUT_MAX_SECONDS`` on the verify phase) so ``total_timeout = min(MAX, max(MIN, N × PER_FILE))``. 4 h dwarfs any realistic hash duration even on multi-million-file workloads and stays four orders of magnitude below the OS limit. Reproducer: a 262 654-file profile failed at the manifest phase in seconds; same profile now completes the integrity manifest normally.
- **MSI install / uninstall: brief PowerShell console flash during the Defender-exclusion CustomAction.** The deferred ``AddDefenderExclusion`` (install) and ``RemoveDefenderExclusion`` (uninstall) launched ``powershell.exe`` directly, which made the host create a console window for ~200 ms before evaluating the script — visible as a black rectangle that flashed mid-install / mid-uninstall and looked like the installer was about to crash. The CustomActions now pass ``-WindowStyle Hidden`` so the PowerShell host starts with its window already off-screen and no flicker reaches the desktop.

### Changed (UI polish)
- **Run-tab Log search box removed.** The live filter at the top of the Log frame was visually heavy for the 90 % case where the user just watches a backup tick by; the same lookup is achievable by expanding the Skipped subtree and scanning the relevant category. Easy to reintroduce later if a real need surfaces.
- **Success-row green tint dropped.** Lines like ``Verification OK: 27/27 files verified`` and ``Backup complete: 27 files in 0.7 s`` no longer paint themselves green in the Log — the green ``✓ Success — Just now · N files`` pill at the top of the Run tab is the canonical success indicator and the in-Log echo was visual duplication.
- **``Log`` LabelFrame title removed.** The bold ``Log`` heading above the tree was redundant: the Log frame is the only multi-row scrollable widget on the Run tab so there is no ambiguity. ``ttk.Frame`` replaces ``ttk.LabelFrame`` for that section.

### Tests
- +2 tests in ``tests/unit/test_hash_phase_timeout.py`` (``TestTimeoutOverflowCap``) pin the cap on huge-workload computations: 262 654 files (manifest) and 1 000 000 files (verifier) both clamp to ``MAX_SECONDS`` and stay well below the OS DWORD-ms ceiling. Guards against a regression of the 3.4.0/3.5.0 overflow.

## [3.5.0] - 2026-05-10

### Changed
- **Run-tab Log widget completely rebuilt as a hierarchical Treeview** to mirror the Schedule journal styling and answer the recurring user question *"is my file X backed up or not?"*. The previous flat ``tk.Text`` (dark terminal palette, monospace, free-form lines) is replaced with a structured ``ttk.Treeview`` (clear background, Segoe UI, expandable nodes). Coloration of warning / error / success rows is done with discreet pale-tinted tags so the visual cue is present without painting the log a sapin de Noël. Auto-scroll to the latest entry is preserved. The tree replaces — does not augment — the old widget; ``log_text`` is gone.
- ``PhaseLogger.info / warning / error`` accept a new keyword-only ``details: dict | None`` argument that travels on the LOG event. ``None`` keeps the legacy flat-line rendering for the ~50 existing call sites. The new parameter is documented in the docstring as the contract between the pipeline and the UI's hierarchy renderer.
- ``collector.py`` now records every excluded path with the **rule that caught it**. The new helper ``_match_excluded`` returns the matched pattern; ``_is_excluded`` is preserved as a thin bool wrapper for backward-compatibility with existing callers and tests. Excluded *directories* are recorded as a single entry (matching the pre-existing don't-recurse-into-excluded-dirs behaviour) — a single ``node_modules`` line in the log instead of fifty thousand file rows.
- ``_SkippedPaths`` retired the ``_SKIPPED_SAMPLE_LIMIT = 5`` cap on its accumulated lists. The old cap kept the in-Log expansion useless on real workloads (the user could only see 5 examples of any category). Memory cost of unlimited retention is bounded by the disk itself; on a pathological 100 k-skipped scenario the accumulator stays under ~30 MB.
- The collector now emits a **single** ``Skipped N file(s) not backed up`` event at INFO level with a structured ``details`` payload (``permission_denied`` / ``os_errors`` / ``excluded_by_pattern``) instead of two separate one-liners. The Run-tab Log unpacks the payload into the category-by-extension hierarchy at render time.

### Added
- ``src/core/file_categorizer.py`` — maps file paths to user-friendly categories by extension. 7 categories (Documents / Photos / Videos / Music / Archives / Code & data / Other), ~250 extensions covering the common formats plus all major RAW makers (Canon ``.cr2/.cr3/.crw``, Nikon ``.nef/.nrw``, Sony ``.arw/.srf/.sr2``, Adobe DNG, Fuji, Olympus, Pentax, Hasselblad, Phase One, ...). The lookup is purely lexical (``Path.suffix.lower()`` → dict) so categorising tens of thousands of paths is essentially free.
- **Run-tab Log search box**: a live ``Entry`` at the top of the Log frame filters the whole tree as the user types. A row stays visible if its own text matches OR if any of its descendants do — a hit on a deep path keeps every parent open, so the user types the file name they care about and immediately sees where it landed (or that it landed nowhere — i.e. it was backed up). Case-insensitive, substring match.
- **Lazy materialisation of the Skipped subtree**: the per-category, per-extension, per-path widgets are only created when the user expands a category. Avoids inserting ~100 k Tk widgets up front on pathological workloads.
- **Reason-of-skip column**: every leaf row in the Skipped tree shows ``permission denied`` / ``excluded: <pattern>`` / ``OS error: …`` in muted grey to the right of the path. The user can tell at a glance which rule sent each file to the skip pile.
- ``Applying exclude patterns (N)`` is now an expandable parent whose children are the individual patterns — replaces the previous single-line ``Applying exclude patterns: *.tmp, *.log, ~$*, …`` which got truncated past 4-5 patterns.

### Tests
- +30 tests in ``tests/unit/test_file_categorizer.py``: every category gets a parametrized batch covering its main extensions, plus guards on the ambiguous ``.ts`` (TypeScript vs MPEG-TS) decision, the case-insensitive lookup, the Path-vs-str input contract, and the documented display order of categories.
- ``tests/unit/test_collector_skipped_aggregation.py`` rewritten for the new payload-based architecture: tests for the uncapped accumulator, the single-event aggregation, the structured ``details`` payload (including the excluded-by-pattern ``(path, pattern)`` tuples), the directory-not-recursed contract, and the new ``Applying exclude patterns`` event with its ``patterns`` list. The legacy "two messages, count + sample in the message text" assertions are removed — the count and samples now live in ``details``, which the UI consumes.

### Changed
- **Wizard step 4 ("How often should we backup?") aligned with step 3's layout.** The previous design used three large picture cards (Daily/Weekly/Monthly) with calendar emojis — visually attractive but stylistically inconsistent with the rest of the wizard which uses ``LabelFrame`` + vertical radios everywhere else (``Storage type``, ``Configuration``). The redesign keeps the wizard look coherent end-to-end:
  - ``Frequency`` ``LabelFrame`` with three vertical radios (``Daily``, ``Weekly``, ``Monthly``).
  - ``Configuration`` ``LabelFrame`` whose contents adapt to the selected frequency:
    - **Daily** → ``Time:`` only.
    - **Weekly** → ``Day:`` (Monday–Sunday combobox) + ``Time:``.
    - **Monthly** → ``Day of month:`` (spinbox 1–31, scheduler clamps to month length on short months) + ``Time:``.
  - The ``Time:`` field accepts ``HH:MM`` (24-hour); the scheduler tab already validates the format at apply time so the wizard does not duplicate the check.
- ``_create_profile`` consumes the new ``schedule_time`` / ``schedule_day_of_week`` / ``schedule_day_of_month`` keys instead of hard-coding ``time="10:00"``. Defaults match ``ScheduleConfig`` so any legacy code path that bypasses the wizard navigation still produces a valid profile.

## [3.4.0] - 2026-05-10

### Added
- **Wizard step 2 — "Common locations" quick-add buttons.** The "What to back up?" step now shows a labelled section under the Add/Remove row with one-click buttons for ``Documents``, ``Pictures``, ``Music`` and ``Videos`` — the four standard Windows user folders that account for the bulk of novice-user backup intent. Clicks reuse the existing idempotent ``_wizard_add_source`` so a second click on the same button is a no-op. Paths resolve via ``Path.home()``; the technical folder names stay English on disk regardless of the OS display language, so this works on every Windows locale without ``SHGetKnownFolderPath`` ceremony.
- ``scripts/bench_copy.py`` — reproducible USB throughput benchmark that compares production ``shutil.copy2`` (kernel ``CopyFileExW``) against a pure-Python read/write loop on a configurable workload. Mirrors the v3.3.15 → v3.3.18 regression shape (many small files) and exits non-zero below a configurable ``--threshold-mbs`` floor. Designed to be run manually before tagging any release that touches ``write_flat`` or its callers, since pytest on ``tmp_path`` (NVMe) cannot detect kernel-vs-loop differences.

### Fixed
- **Manifest and verify phases now enforce a userspace deadline on their parallel hash pool.** A worker stuck inside ``compute_sha256`` — locked file, antivirus mid-scan, OneDrive placeholder rehydrating, NAS share that drops mid-read — would previously hang the whole pipeline indefinitely because Windows file I/O has no kernel-level timeout. The user perceived a frozen UI with no progress and ended up killing the app. Both phases now compute a budget of ``max(60 s, N_files × 30 s)`` and pass it as ``as_completed(timeout=...)``. ``build_integrity_manifest`` raises a ``RuntimeError`` listing the pending files and the likely culprit (AV / locked file / unresponsive share). ``verify_backup`` surfaces the timeout as a graceful ``(False, "...timed out...")`` rather than an exception, since at that stage the destination bytes are already on disk and the backup itself is intact — only the re-check did not complete. The pool is shut down with ``cancel_futures=True`` so the failure path returns promptly even when a worker thread is still inside a kernel I/O wait (Python cannot forcibly kill a thread; the stuck worker drains in the background once the OS releases the lock).

### Tests
- **+37 tests covering four previously-uncovered risk surfaces, all caught earlier by the test-gap audit:**
  - ``tests/unit/test_long_paths_pipeline.py`` (× 6, Windows-only): end-to-end coverage of >260-char paths through ``compute_sha256``, ``build_integrity_manifest``, ``write_flat`` and a full hash → write → re-hash round-trip. Pre-existing coverage was limited to ``safe_remove_tree``; the write/hash side was untested.
  - ``tests/unit/test_integrity_check_dpapi.py`` (× 13): exercises the real ``_get_hmac_key`` (bypassing the conftest autouse mock via a module-level alias captured at import time) — fresh install, DPAPI marker recognition, unwrap, fallback raw key when ``_dpapi_wrap`` fails (must NOT prepend the marker, or the next read loops forever), regen on unwrap failure, malformed file, legacy 32-byte plain file, and POSIX behaviour. Closes the gap on ``src/security/integrity_check.py`` which was excluded from coverage.
  - ``tests/unit/test_filter_state_poison.py`` (× 10): same-size content change (must re-hash, not skip), mtime-not-consulted (live mtime older than recorded must still re-include on hash mismatch), delete + recreate, rename, manifest JSON corruption (degrades to full backup), manifest entry missing the ``hash`` key, ``OSError`` mid-hash drops the file from ``changed`` instead of fail-fasting downstream.
  - ``tests/unit/test_rotator_calendar_edges.py`` (× 8): DST forward/backward (UTC normalisation must survive the spring/autumn shift), naive-``now`` ↔ aware-UTC equivalence (the production ``rotator.py:117`` defensive normalisation gets an explicit pin), non-UTC mtime round-trip (Eastern-authored backup lands on the right UTC day), clock skew (mtime > now does not crash, newest is still kept), and the strict-less-than monthly window boundary (gfs_monthly=12 + dt = exactly 12 months ago → pruned, off-by-one contract).
- **+6 tests in ``tests/unit/test_hash_phase_timeout.py``** for the deadline fix: timeout fires with a clear ``RuntimeError`` listing pending files (manifest), surfaces as a graceful ``(False, "...timed out...")`` (verifier), happy path remains unaffected for both phases, and pool shutdown does not block on stuck workers (measured: failure path returns in <3 s when workers would otherwise sleep 10 s).
- Net: +43 tests, 0 regressions on the 146-test sample of touched domains (manifest, verifier, pipeline, filter, integrity, encryption, rotator).

## [3.3.21] - 2026-05-09

### Changed
- Permission-denied skip summary in ``src/core/phases/collector.py`` now surfaces at ``INFO`` level with reassuring wording (``"Skipped N protected item(s) — typically system caches or files locked by another app (this is normal, no action needed). Examples: …"``) instead of a yellow ``WARNING`` saying ``"permission denied"``. On workloads with many cache directories the warning was leading novice users to believe their files weren't being backed up — the message is benign in 99 % of cases (``.pytest_cache``, WardSOAR ``volatile`` evidence stores, files locked by Outlook/SQL/etc.) and now reads accordingly.
- ``OS error`` skip summary stays at ``WARNING`` — those may indicate real disk/filesystem issues and deserve attention.

### Added
- New ``INFO`` line at the start of every collect phase: ``"Applying exclude patterns: <comma-separated list>"``. Surfaces the active filter list in the run log so a user who sees the skip summary can audit what is being excluded without digging through the profile dialog. Skipped when the exclude list is empty (no noise).

### Tests
- +5 tests in ``tests/unit/test_collector_skipped_aggregation.py``: ``TestUserFriendlyWording`` (× 3, asserts the level demotion, the reassuring keywords, and that ``OS error`` stays at ``WARNING``) and ``TestExcludePatternsLogged`` (× 2, asserts the new ``Applying exclude patterns`` line and its silence on an empty list). 4 existing tests adapted to the new level/format. Full suite at 1 782 passed / 0 failed.

## [3.3.20] - 2026-05-09

### Removed
- ``copy_and_hash`` (in ``src/core/hashing.py``) and ``write_flat_with_hashes`` (in ``src/core/phases/local_writer.py``). Both were orphaned by the v3.3.19 pipeline rework that moved hashing out of the writer's inner loop into a parallel ``_phase_integrity`` pass. Production callers had already migrated to ``compute_sha256`` + ``write_flat``; the two helpers were kept "for tests" and accumulated documentation that no longer matched the actual code path. Their tests (~140 lines across ``test_hashing.py`` and ``test_local_writer.py``) were removed at the same time — there is no behavioural surface left to cover.
- Module-level ``shutil`` import in ``src/core/hashing.py`` (was only used by the deleted ``copy_and_hash``).

### Fixed
- ``AutoStart.ensure_startup`` (Windows registry auto-start) is now idempotent. It is invoked after every ``save_profile`` for a refresh, and the previous code wrote the registry value and emitted ``Auto-start configured via registry: …`` every time even when nothing had changed — flooding the run log with duplicate lines (12 of them in a single morning's logs). The new path queries the existing value first and short-circuits when it already matches the desired command; ``SetValueEx`` and the INFO log are emitted only on actual change.

### Added
- Two new default exclude patterns on ``BackupProfile``:
  - ``.pytest_cache`` (basename style) — every Python project drop-zone for pytest's incremental cache, locked while pytest runs and consistently rejected by the collector with "permission denied".
  - ``*/evidence/*/volatile`` (path style) — WardSOAR-style volatile-memory dumps under ``evidence/<uuid>/volatile``, owned by a live process and always inaccessible during collection.
- Path-style exclude pattern matching in ``src.core.phases.collector._is_excluded``. Patterns containing ``/`` are matched against the source-relative POSIX path (gitignore-style); patterns without ``/`` keep the legacy basename-anywhere behaviour. The existing default patterns (``__pycache__``, ``*.tmp``, ``.git``, ``node_modules``, …) are unchanged in semantics. ``_is_excluded`` gains a new optional ``source_root`` parameter; callers in ``collect_files`` and ``_collect_directory`` pass it.

### Comments / docs
- ``src/core/hashing.py`` header and ``HASH_CHUNK_SIZE`` comment block rewritten to describe the v3.3.19 pipeline (parallel source hash in Phase 3, kernel ``shutil.copy2`` in Phase 4) instead of the obsolete "v3.3.18 single-pass ``copy_and_hash``" story.
- Test docstrings in ``test_disk_full_scenarios.py`` and ``test_write_error_failfast.py`` updated to mock ``shutil.copy2`` (the actual writer primitive) rather than the deleted ``copy_and_hash``.

### Tests
- +7 tests (idempotence of ``ensure_startup``, default exclude patterns, path-style pattern matching, basename-vs-path coexistence). Net change after the dead-code test cleanup: −280 lines of obsolete coverage, full suite at 1 748 passed / 0 failed.

## [3.3.19] - 2026-05-09

### Fixed
- USB throughput on small-file workloads (~30 k files) was capped at ~8 MB/s in v3.3.18 because the writer phase was still hashing each file synchronously inside its inner loop (``copy_and_hash`` did ``compute_sha256(src)`` then ``shutil.copy2`` per file). The hash dominated the loop and serialised the kernel copy with the SHA-256 computation. Pipeline now restored to its v3.3.14 shape: ``_phase_integrity`` runs BEFORE ``_phase_write`` for **all** destinations and hashes every source in parallel via ``manifest.py``'s ``ThreadPoolExecutor`` (4-8 workers); ``_phase_write`` then becomes a pure ``shutil.copy2`` loop with nothing in the inner pass, so the kernel copy primitive saturates the USB pipe. Visually the user now sees ``Building integrity manifest...`` BEFORE ``Copying to Storage...`` in the run log, instead of the previous opaque "Copying" with hashes hidden inside.

### Changed
- ``_phase_integrity`` no longer consumes hashes from a writer cache; it always hashes from the source. This made the v3.3.15-style "writer fills ``ctx.file_hashes`` for the manifest phase" plumbing obsolete and removed an inter-phase coupling that was both a maintenance burden and the root cause of the regression chain v3.3.15→v3.3.18.
- ``write_flat`` now uses ``shutil.copy2`` directly (kernel-space) and carries an ``INVARIANT`` comment block forbidding its replacement by a Python read/write loop without a fresh USB benchmark. ``write_flat_with_hashes`` is kept as a compatibility shim for tests; it now calls ``write_flat`` then re-hashes the destination.

## [3.3.18] - 2026-05-09

### Fixed
- Restored the v3.3.14-class corruption-detection guarantee that v3.3.15–v3.3.17 had silently weakened. ``copy_and_hash`` now hashes the **source** before delegating the byte transfer to ``shutil.copy2``, instead of hashing the destination after the copy. The manifest therefore records what the user wanted to back up, and the ``verify`` phase later catches any divergence between source and destination — whether caused by a flaky USB controller flipping bits during the copy, a bad cable, or a source mutation that produced a Frankenstein destination. Previously, hashing the destination meant a corrupted copy would hash to its own corruption and the backup would be silently accepted as valid. Performance is unchanged from v3.3.17 because the source pre-pass read is served almost entirely by the OS file-system cache (the bytes are still hot when ``shutil.copy2`` calls ``CopyFileExW`` immediately after).

### Added
- ``src/core/hashing.py::copy_and_hash`` now carries an explicit ``INVARIANT`` comment block documenting that the byte transfer MUST stay on a kernel primitive (``shutil.copy2``). A fresh USB benchmark is required before swapping that delegation for a Python loop. This is the first rule of the upcoming ``docs/INVARIANTS.md`` register and is meant to prevent the v3.3.15-style throughput regression from happening again.

## [3.3.17] - 2026-05-09

### Fixed
- USB throughput on small-file workloads remained capped (~1–10 MB/s) even after the 3.3.16 chunk-size bump, because the bottleneck on a 30 k+ small-file backup is per-file syscall overhead (open + open + Python loop + ``copystat`` + close × 2) — not chunk size. ``copy_and_hash`` now delegates the actual transfer to ``shutil.copy2``, which on Windows resolves to ``CopyFileExW`` (kernel-space, single transaction for open + transfer + metadata) and on Linux uses ``sendfile``/``copy_file_range`` where supported. The SHA-256 is computed from the destination after the copy, not from the source during the copy: the destination is frozen at that point, so the manifest still describes exactly what is on disk and the anti-TOCTOU guarantee that 3.3.15 introduced is preserved. The OS file-system cache absorbs the cost of the read-back for small files (their bytes are still hot in RAM), so the post-pass hash is near-free; large files pay one additional linear read but the savings on the rest of the workload swamp it.

## [3.3.16] - 2026-05-08

### Added
- Live progress dialog when deleting a profile's backups. The previous flow closed the "Also delete all backups?" confirmation immediately on Yes and ran the sweep silently on a background thread, leaving the user with no way to know whether the app was working or had hung. The new modal keeps a determinate 0..N progress bar visible — ``delete_profile_backups`` now pre-counts matches across every reachable destination so the bar can be accurately driven — and shows the backup name currently being processed. The dialog auto-closes 500 ms after the last delete (the brief 100 % hold gives the user a chance to register the success state before the modal vanishes). The OS close button is neutralised while the sweep runs so a stray click cannot orphan the worker thread mid-deletion.

### Fixed
- Backup throughput collapsed to ~7 MB/s on external USB SSDs (Samsung T7 and similar) because the new single-pass `copy_and_hash` introduced in 3.3.15 was reading/writing in 128 KiB chunks (`HASH_CHUNK_SIZE`). The 128 KiB value was inherited from the old read-only `compute_sha256` helper, where it was fine; once the same constant drove the write side too, the syscall overhead dominated and capped the USB pipe well below its native ~500 MB/s. Bumped to **4 MiB** — matches the buffer size SSDs/USB devices saturate on, and SHA-256 absorbs the larger chunks without CPU penalty. Restores 3.3.14-class throughput while preserving the anti-TOCTOU single-pass guarantee.

### Changed
- ``delete_profile_backups`` signature: ``progress_callback`` is now ``Callable[[int, int, str], None]`` (current, total, name) instead of ``Callable[[str], None]`` (status string). Existing callers were already passing ``None`` so this change is invisible outside of tests; the new shape is what makes the determinate progress bar above possible.

## [3.3.15] - 2026-05-08

### Added
- Commit-marker module (`.wbcommit`) — destination-side proof of completeness. Each backup is now sealed with a small HMAC-signed JSON sidecar that binds the marker to the backup's `.wbverify` checksum and to the local install's DPAPI-wrapped HMAC key. The presence of a *valid* `.wbcommit` is the **sole authority** for whether a backup on a destination is restorable; without one, the backup is treated as orphaned. Defeats marker transposition (lifting a marker from backup A onto backup B fails the manifest-binding check) and forged markers (signed by a different key are rejected).
- Phase 0 orphan scan — at the start of every run, deletes any backup that lacks a valid commit marker on every reachable destination (primary + mirrors). Skips legacy backends that don't expose `list_orphan_backups` and Object-Lock buckets (the bucket lifecycle rule reclaims those).
- Phase 6.5 commit primary + per-mirror commit phase — write/upload the `.wbcommit` only after the corresponding `.wbverify` has been validated, so a destination that fails verification stays orphan-tagged.
- `LocalStorage.list_orphan_backups()` — drives the phase-0 scan for local destinations.
- `copy_and_hash()` in `src/core/hashing.py` — single-pass SHA-256 + copy that defeats the manifest→write TOCTOU window: source files mutated between hash and write would otherwise produce a manifest that doesn't match what landed on disk.
- `prune_manifest_entries()` + `skipped_files` recording — when a file vanishes between hash and write, the manifest now removes the entry AND records it under `skipped_files` so the verifier and UI can surface the data loss instead of hiding it behind a recomputed checksum.
- `_best_effort_cleanup` and backup-type rollback sentinel — partial backups are reclaimed immediately on the failure path of `run_backup`; a forced-FULL promotion that crashes mid-run never permanently strands the profile in FULL mode.

### Fixed
- Blank main window after clicking "Show window" from the system tray — `pystray` invokes its menu callbacks from a daemon thread, and the previous wiring called `root.deiconify()` / widget mutation directly from that thread. Tk is not thread-safe, so the call sometimes raced with the Tk render loop and produced an empty white window with no widgets. All three tray callbacks (`Show window`, `Run backup now`, `Exit`) now marshal onto the Tk main thread via `root.after(0, ...)` before touching any widget state. Regression test in `tests/unit/test_tray_main_thread_marshalling.py`.
- Test-suite memory leak that hard-crashed the developer machine — `tests/test_manifest_upload.py::TestSFTPDownloadManifest::test_download_raises_when_existing_dst_cannot_be_cleared` patched `shutil.rmtree` while the SFTP code path uses `safe_remove_tree` (`os.unlink`/`os.rmdir` directly). The patch was inert, the cleanup succeeded, and the test then ran `_tar_stream_download` against an unconfigured `MagicMock` SFTP transport — that call grew the Python process to 5+ GB of virtual memory in 20 s and saturated the pagefile, freezing the system. The test now stubs `safe_remove_tree` to return a `RemoveResult` with residuals, exercising the intended error branch in 1 s and 58 MB. Full suite peak memory dropped from >8 GB (crash) to 215 MB.

### Tests
- 125 new tests added across four files: `tests/unit/test_manifest_pruning.py` (17), `tests/unit/test_s3_auto_detect.py` (50), `tests/test_backup_engine_phases.py` (42), and `tests/unit/test_bandwidth_tester.py` (+16 covering `_RandomStream`, `_remote_sync`, Object-Lock guard, fall-back paths).
- Coverage 85 % → 90 %. Modules formerly below the 80 % gate (`s3_setup.py` 50 %, `bandwidth_tester.py` 71 %, `backup_engine.py` 73 %, `manifest.py` 79 %) now sit at 91 %, 99 %, 80 %, and 100 % respectively.

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
