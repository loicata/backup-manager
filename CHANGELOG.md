# Changelog

All notable changes to Backup Manager are documented in this file.

Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
