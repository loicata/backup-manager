#!/bin/bash
# Test suite for assets/server_helper.sh
#
# Runs entirely on the local machine — no SSH, no Pi. Builds a tar
# fixture covering the tricky filename cases (spaces, accents, deep
# nesting, empty files, multi-MB files) and feeds it to the helper.
# Asserts that (a) every byte landed correctly on disk and (b) every
# hash the helper emitted matches a reference computed by sha256sum.
#
# Usage:
#     bash tests/test_server_helper.sh
# Returns:
#     exit 0 on success
#     exit 1 on any failed assertion (with red error line on stderr)
#
# Why a bash test, not pytest:
#     The helper is bash. Its bugs (quoting, set -e, --to-command
#     escaping) only surface when bash is the runner. A pytest wrapper
#     would launch bash anyway and add a layer of escape rules between
#     us and the actual contract. Easier to write the assertions in
#     the same shell that runs the code under test.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HELPER="$ROOT/assets/server_helper.sh"

if [[ ! -x "$HELPER" ]]; then
    chmod +x "$HELPER"
fi

# Sandbox: a unique tmpdir per run so parallel invocations don't
# stomp each other. Cleaned up on EXIT regardless of outcome.
TMP=$(mktemp -d -t bm-helper-test-XXXXXX)
trap 'rm -rf "$TMP"' EXIT

FIXTURE="$TMP/fixture"
EXTRACTED="$TMP/extracted"
HASHES="$TMP/helper_hashes.txt"
REFERENCE="$TMP/reference_hashes.txt"

mkdir -p "$FIXTURE"

# Trace counters for the final summary.
PASS=0
FAIL=0

assert_eq() {
    local label="$1" expected="$2" actual="$3"
    if [[ "$expected" == "$actual" ]]; then
        PASS=$((PASS + 1))
        echo "  PASS  $label"
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL  $label" >&2
        echo "        expected: $expected" >&2
        echo "        actual:   $actual" >&2
    fi
}

assert_file_exists() {
    local label="$1" path="$2"
    if [[ -f "$path" ]]; then
        PASS=$((PASS + 1))
        echo "  PASS  $label"
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL  $label (file missing: $path)" >&2
    fi
}

# -------------------------------------------------------------------
# Build fixture
# -------------------------------------------------------------------
echo "=== Building fixture ==="

# 1. Top-level file
echo "top-level content" > "$FIXTURE/top.txt"

# 2. Nested file
mkdir -p "$FIXTURE/dir1/dir2"
echo "nested content" > "$FIXTURE/dir1/dir2/nested.txt"

# 3. File with spaces in name (matches user's "Loic Perso" source dir)
mkdir -p "$FIXTURE/Loic Perso"
echo "space content" > "$FIXTURE/Loic Perso/file with space.txt"

# 4. File with UTF-8 accent in name
mkdir -p "$FIXTURE/utf8"
echo "accent content" > "$FIXTURE/utf8/café-résumé.txt"

# 5. Empty file (size 0 — hash should be e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855)
touch "$FIXTURE/empty.txt"

# 6. 1 MB file (forces streaming, not single-buffer read)
dd if=/dev/urandom of="$FIXTURE/big.bin" bs=1024 count=1024 2>/dev/null

# 7. Multiple siblings in the same dir
mkdir -p "$FIXTURE/sibs"
echo "a" > "$FIXTURE/sibs/a.txt"
echo "b" > "$FIXTURE/sibs/b.txt"
echo "c" > "$FIXTURE/sibs/c.txt"

# Reference hashes (what sha256sum says about each fixture file)
# We capture them as relative paths matching what tar produces.
#
# Normalisation: sha256sum prefixes the path with ``*`` when it
# reads in binary mode (the default on most platforms for explicit
# paths). The helper's awk-built output uses the canonical
# ``<hash>  <path>`` 2-space form that the Python parser in
# verify_backup_files expects. We strip the ``*`` from the
# reference so the diff compares like for like.
(cd "$FIXTURE" && find . -type f -print0 | xargs -0 sha256sum) \
    | sed -E 's|^([a-f0-9]{64}) \*|\1  |' \
    | sort > "$REFERENCE"

echo "Fixture: $(find "$FIXTURE" -type f | wc -l) files"

# -------------------------------------------------------------------
# Run helper
# -------------------------------------------------------------------
echo "=== Running helper ==="

# Tar the fixture's CONTENTS (not the fixture dir itself) so the
# extracted paths in $EXTRACTED match the relative paths the manifest
# would use. Equivalent to BM's upload_tar_stream which feeds
# rel_path inside a TarInfo.
(cd "$FIXTURE" && tar cf - .) | bash "$HELPER" "$EXTRACTED" > "$HASHES" 2>"$TMP/helper.stderr"

# -------------------------------------------------------------------
# Assertions
# -------------------------------------------------------------------
echo "=== Assertions ==="

# 1. Helper exited cleanly
echo "  PASS  helper exit code 0"
PASS=$((PASS + 1))

# 2. Every fixture file landed in extracted/ with same content
diff -r "$FIXTURE" "$EXTRACTED" >/dev/null && {
    PASS=$((PASS + 1))
    echo "  PASS  extracted tree byte-identical to fixture"
} || {
    FAIL=$((FAIL + 1))
    echo "  FAIL  extracted tree differs from fixture" >&2
    diff -r "$FIXTURE" "$EXTRACTED" >&2 | head -10
}

# 3. Helper hash output is well-formed: every line is 64-hex + 2-space + path
malformed=$(grep -cvE '^[a-f0-9]{64}  .+$' "$HASHES" || true)
assert_eq "every helper line is <64-hex>  <path>" "0" "$malformed"

# 4. Helper count matches fixture file count
helper_count=$(wc -l < "$HASHES")
fixture_count=$(find "$FIXTURE" -type f | wc -l)
assert_eq "helper line count" "$fixture_count" "$helper_count"

# 5. Every helper hash matches the reference for the same path
# Sort helper output the same way as reference for diff
sort "$HASHES" > "$TMP/helper_sorted.txt"
if diff -q "$REFERENCE" "$TMP/helper_sorted.txt" >/dev/null 2>&1; then
    PASS=$((PASS + 1))
    echo "  PASS  every helper hash matches sha256sum reference"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL  helper hashes diverge from sha256sum reference:" >&2
    diff "$REFERENCE" "$TMP/helper_sorted.txt" >&2 | head -10
fi

# 6. Empty file hash is the canonical SHA-256 of zero bytes
empty_line=$(grep "  ./empty.txt$" "$HASHES" || true)
expected_empty="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
actual_empty=$(echo "$empty_line" | awk '{print $1}')
assert_eq "empty file hashes to canonical SHA-256" "$expected_empty" "$actual_empty"

# 7. File-with-spaces was extracted correctly and its hash output preserves the spaces
space_path="./Loic Perso/file with space.txt"
if grep -qF "  $space_path" "$HASHES"; then
    PASS=$((PASS + 1))
    echo "  PASS  helper preserves spaces in path: '$space_path'"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL  helper output missing space path (expected '  $space_path')" >&2
    echo "       Output sample:" >&2
    head -3 "$HASHES" >&2
fi

# 8. UTF-8 accent path round-trips
utf8_path="./utf8/café-résumé.txt"
if grep -qF "  $utf8_path" "$HASHES"; then
    PASS=$((PASS + 1))
    echo "  PASS  helper preserves UTF-8 in path"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL  helper output missing UTF-8 path" >&2
fi

# 9. 1 MB file extraction: size on disk = 1 MB
big_size=$(stat -c%s "$EXTRACTED/big.bin" 2>/dev/null || stat -f%z "$EXTRACTED/big.bin")
assert_eq "1 MB file extracted with correct size" "1048576" "$big_size"

# 10. Stderr was empty (no warnings, no errors)
stderr_size=$(wc -c < "$TMP/helper.stderr")
assert_eq "helper stderr is empty" "0" "$stderr_size"

# 11. POSIX compliance check (regression guard for the 2026-05-14 bug).
#
# tar --to-command invokes /bin/sh for the child command, not bash.
# On Debian/Ubuntu/Raspberry Pi OS /bin/sh is dash, which rejects the
# bash-only ``[[ ... ]]`` test. Using ``[[`` inside --to-command made
# every file extraction silently no-op while sha256sum still emitted
# correct hashes — sidecar full, backup dir empty. The bug went
# undetected for an entire release cycle because Git Bash on Windows
# (where this test runs locally) symlinks /bin/sh to bash, so the
# corruption never surfaced.
#
# Static check: ensure no ``[[`` appears in the --to-command body of
# the helper. We scope the search to the heredoc-style ``--to-command='
# ... '`` block to avoid false positives in comments / docstrings.
to_cmd_body=$(awk "/--to-command='/,/^'/" "$HELPER" | grep -v "^#")
if echo "$to_cmd_body" | grep -q '\[\['; then
    FAIL=$((FAIL + 1))
    echo "  FAIL  POSIX violation: [[ ]] used inside tar --to-command body" >&2
    echo "        That body runs under /bin/sh (dash on Debian); use [ ]." >&2
    echo "$to_cmd_body" | grep -n '\[\[' >&2
else
    PASS=$((PASS + 1))
    echo "  PASS  no [[ inside tar --to-command body (POSIX-safe)"
fi

# 12. Cross-shell extraction test: re-run the helper via plain dash
# when available. The helper's outer wrapper uses bashisms
# (``set -o pipefail``) that dash ignores with a warning — that is
# fine in production because the helper's shebang ``#!/bin/bash`` is
# honoured. What we check here is the integrity of extraction itself:
# the ``tar --to-command`` body MUST work under dash because that is
# what /bin/sh evaluates it as on Debian/Ubuntu/Raspberry Pi OS, and
# any [[ ]] or [ -v ] regression there silently empties the backup
# (see the 2026-05-14 incident).
#
# Hash integrity matters more than the extracted tree alone: a helper
# that creates empty files but emits the correct hashes would still
# defeat the verify, so we also diff the hashes against the bash run.
if command -v dash >/dev/null 2>&1; then
    DASH_EXTRACTED="$TMP/extracted_dash"
    DASH_HASHES="$TMP/hashes_dash.txt"
    (cd "$FIXTURE" && tar cf - .) | dash "$HELPER" "$DASH_EXTRACTED" > "$DASH_HASHES" 2>/dev/null
    dash_files=$(find "$DASH_EXTRACTED" -type f 2>/dev/null | wc -l)
    expected_files=$(find "$FIXTURE" -type f | wc -l)
    assert_eq "helper extracts under dash" "$expected_files" "$dash_files"
    # Same hash output regardless of which shell ran the outer wrapper.
    sort "$DASH_HASHES" > "$TMP/dash_sorted.txt"
    if diff -q "$REFERENCE" "$TMP/dash_sorted.txt" >/dev/null 2>&1; then
        PASS=$((PASS + 1))
        echo "  PASS  dash run produces same hashes as reference"
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL  dash run hashes diverge from reference" >&2
        diff "$REFERENCE" "$TMP/dash_sorted.txt" >&2 | head -10
    fi
else
    echo "  SKIP  dash not installed; skipping real-POSIX cross-shell test"
fi

# -------------------------------------------------------------------
# Summary
# -------------------------------------------------------------------
echo ""
echo "=== Summary ==="
echo "  $PASS passed, $FAIL failed"
if [[ $FAIL -gt 0 ]]; then
    exit 1
fi
