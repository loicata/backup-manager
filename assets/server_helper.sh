#!/bin/bash
# Backup Manager server helper — hash-during-upload integration point.
#
# Replaces ``tar xf - -C <dest>`` on the SSH server. Extracts the TAR
# stream AND emits one SHA-256 hash per extracted regular file, all in
# a single pass over the wire data — no second disk read.
#
# Usage:
#     ssh server "/path/to/server_helper.sh <dest_dir>" < local_tar_stream > local_hashes.txt
#
# The hash output format on stdout is one line per regular file:
#     <64-hex-sha256>  <relative-path-inside-dest>
# matching the standard ``sha256sum`` line shape so callers can
# parse it the same way.
#
# Stderr carries diagnostic messages (extraction errors, unsupported
# file types). Callers should keep stderr separate and surface it
# only on a non-zero exit.
#
# Requirements on the server:
#   - GNU tar (the ``--to-command`` extraction hook is GNU-only;
#     BSD tar's ``--to-stdout`` is NOT a substitute)
#   - bash 4.x or later (for ``set -o pipefail``)
#   - coreutils ``sha256sum``, ``tee``, ``awk``, ``mkdir``
#
# Exit codes:
#   0  — extraction complete, every regular file hashed
#   1  — usage error (no dest dir provided)
#   2  — destination directory could not be created
#   *  — propagated from tar (non-zero means partial extraction)
#
# Design notes:
#   - We deliberately ``cd`` into ``$DEST`` so all paths in the hash
#     output are relative to the destination, matching what the
#     client's manifest stores. Absolute paths would force the
#     client to strip a prefix and the prefix would depend on the
#     SSH user's home — fragile.
#   - The inner command for ``--to-command`` runs in a NEW shell per
#     extracted entry. We keep it small so the fork/exec overhead
#     stays low on large backups (231 k files × forkexec is the dominant
#     cost on commodity hardware, so the inner command must do as
#     little setup as possible).
#   - ``tee`` is the trick that lets us hash without reading from disk
#     a second time: the file content flows from tar's stdin through
#     ``tee`` which writes it to disk AND pipes it to ``sha256sum``.
#     One stream, two consumers.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $(basename "$0") <dest_dir>" >&2
    exit 1
fi

DEST="$1"

if ! mkdir -p "$DEST" 2>/dev/null; then
    echo "error: could not create destination '$DEST'" >&2
    exit 2
fi

cd "$DEST"

# Per-entry extraction hook. The case dispatches on TAR_FILETYPE:
#   f  — regular file: create parent dirs, tee content to disk + hash
#   d  — directory: create it, no hash
#   *  — anything else (symlinks, devices, hard links): warn and skip
#         (BM doesn't back these up so we should never see them; if
#         a future BM version starts including them, the warning
#         surfaces in the SSH stderr which the client logs).
#
# The single-quoted command body is passed verbatim to a child shell
# spawned by tar — the env vars TAR_FILETYPE and TAR_REALNAME are set
# by tar inside that shell.
#
# CRITICAL: tar --to-command invokes /bin/sh (NOT bash) for the child
# command. On Debian/Ubuntu/Raspberry Pi OS /bin/sh is dash, which
# does NOT understand the bash-only ``[[ ... ]]`` test syntax. Using
# ``[[`` here would fail silently at runtime — the test would crash,
# mkdir -p would never run, tee would fail to open the file, BUT
# sha256sum would still consume stdin and emit a (valid!) hash. The
# result: a sidecar full of correct hashes but an empty backup dir.
# Stick to POSIX ``[ ... ]`` here, no exceptions.
tar xf - --to-command='
    case "$TAR_FILETYPE" in
        f)
            parent=$(dirname "$TAR_REALNAME")
            if [ "$parent" != "." ]; then
                mkdir -p "$parent"
            fi
            tee "$TAR_REALNAME" | sha256sum | awk -v p="$TAR_REALNAME" "{print \$1 \"  \" p}"
            ;;
        d)
            mkdir -p "$TAR_REALNAME"
            ;;
        *)
            echo "warning: skipping non-regular entry type=$TAR_FILETYPE path=$TAR_REALNAME" >&2
            ;;
    esac
'
