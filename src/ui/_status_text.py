"""Status-line truncation helper shared by Run-tab and Verify-tab.

Tk ``ttk.Label`` does not clip its own text, so a 200-character path
requests a 200-character-wide label and pushes the right-anchored
percent counter off-screen even when ``percent_label`` is packed
``side="right"``. This helper caps the displayed text and keeps the
END of the path visible (basename + a few parents) since that is the
part the user needs to identify the current file at a glance.

The same module is imported by both progress consumers so a tweak to
the truncation rule (max width, leading ellipsis style) propagates
to every tab in one edit.
"""

from __future__ import annotations

# Maximum characters displayed in the "phase: filename" status line.
# Beyond this length the path is truncated with a leading ellipsis so
# the percent label on the right of the same row stays visible.
# Calibrated for a ~1400 px window with the default Tk font: 80 chars
# leaves ~6-8 chars of empty pad before the % column even on smaller
# screens.
STATUS_MAX_CHARS: int = 80


def truncate_status_text(phase: str, filename: str, max_chars: int = STATUS_MAX_CHARS) -> str:
    """Build a "phase: filename" line that never exceeds ``max_chars``.

    The truncation keeps the **end** of the path (basename + a few
    parents) which is the part the user actually wants to see -- the
    leading components are replaced with ``...``.

    Examples:
        >>> truncate_status_text("hashing", "a.txt", max_chars=80)
        'hashing: a.txt'
        >>> truncate_status_text("hashing", "/very/long/path/...long.../mail.eml", max_chars=30)
        'hashing: ...g.../mail.eml'

    Args:
        phase: Phase name (``hashing``, ``upload``, ``Verifying`` ...).
            May be empty -- the line becomes just the filename.
        filename: File path being processed. May be empty -- the line
            becomes just the phase string clipped to ``max_chars``.
        max_chars: Hard cap on the returned string length.

    Returns:
        Display-ready status line bounded by ``max_chars``.
    """
    if not filename:
        return phase[:max_chars]
    full = f"{phase}: {filename}" if phase else filename
    if len(full) <= max_chars:
        return full

    prefix = f"{phase}: ..." if phase else "..."
    # If even the prefix does not fit, hard-clip from the right. This
    # is a degenerate case (phase name itself > max_chars) -- better
    # to truncate the phase than crash the layout.
    tail_budget = max_chars - len(prefix)
    if tail_budget < 1:
        return full[:max_chars]
    return prefix + filename[-tail_budget:]
