"""Email config propagation between profiles.

A backup user typically uses the SAME SMTP server and recipient
across every profile (their personal notification target). Asking
them to re-type the full SMTP credentials on every new profile is
friction that produces typos and asymmetric configurations. This
module centralises the two helpers that drive the auto-fill UX:

* ``pick_email_source(profiles)`` — chooses an existing profile
  whose email config can seed a new one.
* ``propagate_email_to_unconfigured(source, others)`` — replicates
  ``source.email`` into every OTHER profile whose email is not yet
  configured. Profiles that already carry an email are left alone
  so the user keeps full control once they have explicitly
  customised a profile.

"Configured" is defined by ``EmailConfig.username`` being non-empty
(stripped of surrounding whitespace). The SMTP authentication
username is the strongest "the user filled SMTP in" signal: an
enabled email with no username would fail to send, so users always
set it when they configure SMTP.
"""

from __future__ import annotations

import copy
from collections.abc import Iterable

from src.core.config import BackupProfile, EmailConfig


def is_email_unconfigured(email: EmailConfig) -> bool:
    """Return True when ``email`` lacks a usable SMTP username.

    Whitespace-only usernames are treated as empty so a stray space
    cannot accidentally lock a profile out of propagation.
    """
    return not email.username.strip()


def pick_email_source(
    profiles: Iterable[BackupProfile],
) -> BackupProfile | None:
    """Return the first profile in ``profiles`` whose email is configured.

    Iteration order is preserved — the caller decides priority
    (typically sidebar order: top-most active profile wins). Returns
    ``None`` when no profile carries an email yet, in which case the
    caller should leave the new profile's email at its default.
    """
    for p in profiles:
        if not is_email_unconfigured(p.email):
            return p
    return None


def propagate_email_to_unconfigured(
    source_profile: BackupProfile,
    other_profiles: Iterable[BackupProfile],
) -> list[BackupProfile]:
    """Copy ``source_profile.email`` into every unconfigured profile.

    "Unconfigured" means ``email.username`` is empty (stripped). A
    profile that already carries an email is preserved verbatim —
    the user has explicitly customised it and we do not overwrite
    their intent.

    The source profile is identified by ``id`` and skipped if it
    appears in ``other_profiles`` (defensive against the caller
    forgetting to filter). The source profile's own email config is
    never mutated.

    Args:
        source_profile: The profile that just had its email saved
            (or the seed profile selected via ``pick_email_source``).
        other_profiles: Every OTHER known profile. The function
            still filters out the source by id for robustness.

    Returns:
        The list of profiles whose ``.email`` was REPLACED, in
        iteration order. The caller is responsible for persisting
        them (typically via ``config_manager.save_profile``) — this
        module stays I/O-free so it remains unit-testable without
        a tmp dir.
    """
    if is_email_unconfigured(source_profile.email):
        return []

    mutated: list[BackupProfile] = []
    for p in other_profiles:
        if p.id == source_profile.id:
            continue
        if not is_email_unconfigured(p.email):
            continue
        # Deep copy so a subsequent edit to ``source_profile.email``
        # cannot leak into the propagated copies (the user's intent
        # was a one-shot fan-out, not a live binding).
        p.email = copy.deepcopy(source_profile.email)
        mutated.append(p)
    return mutated
