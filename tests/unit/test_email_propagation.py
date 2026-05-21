"""Tests for cross-profile email config propagation.

UX requirement (user, 22/05/2026):

* When a new profile is created and another existing profile already
  has an email configured, auto-fill the new profile's email with
  that one.
* When the user adds an email to a profile, propagate the same email
  to every OTHER profile whose email is not yet configured (i.e.
  whose SMTP ``username`` is empty). Profiles that already carry an
  email keep it.

"Configured" = ``EmailConfig.username`` is non-empty (the SMTP
authentication username is the strongest signal that the user
actually filled SMTP in; an enabled email with no username would
fail to send anyway, so users always set it when they configure
SMTP).

These tests pin both helpers in ``src.core.email_propagation``.
"""

from __future__ import annotations

import pytest

from src.core.config import BackupProfile, EmailConfig
from src.core.email_propagation import (
    is_email_unconfigured,
    pick_email_source,
    propagate_email_to_unconfigured,
)


def _profile(name: str, email: EmailConfig | None = None) -> BackupProfile:
    p = BackupProfile(name=name)
    if email is not None:
        p.email = email
    return p


def _filled_email(username: str = "alice@example.io") -> EmailConfig:
    return EmailConfig(
        enabled=True,
        smtp_host="smtp.example.io",
        smtp_port=587,
        use_tls=True,
        username=username,
        password="secret",
        from_address=username,
        to_address=username,
        send_on_success=True,
        send_on_failure=True,
    )


class TestIsEmailUnconfigured:
    """``username`` empty → unconfigured. Other fields are irrelevant."""

    def test_default_is_unconfigured(self) -> None:
        assert is_email_unconfigured(EmailConfig()) is True

    def test_whitespace_only_username_is_unconfigured(self) -> None:
        # Defensive: stripped username so a stray space doesn't
        # accidentally lock a profile out of propagation.
        assert is_email_unconfigured(EmailConfig(username="   ")) is True

    def test_username_set_is_configured(self) -> None:
        assert is_email_unconfigured(_filled_email()) is False

    def test_smtp_host_set_but_no_username_is_unconfigured(self) -> None:
        """Half-filled config (host set, username not) does NOT count."""
        cfg = EmailConfig(smtp_host="smtp.example.io")
        assert is_email_unconfigured(cfg) is True


class TestPickEmailSource:
    """``pick_email_source`` returns the first configured profile in
    iteration order (caller decides ordering — typically sidebar order).
    """

    def test_returns_none_when_no_profile_configured(self) -> None:
        profiles = [_profile("A"), _profile("B"), _profile("C")]
        assert pick_email_source(profiles) is None

    def test_returns_first_configured_in_order(self) -> None:
        profiles = [
            _profile("A"),
            _profile("B", _filled_email("bob@example.io")),
            _profile("C", _filled_email("carol@example.io")),
        ]
        result = pick_email_source(profiles)
        assert result is not None
        assert result.name == "B"

    def test_single_configured_profile_is_returned(self) -> None:
        profiles = [_profile("A", _filled_email())]
        result = pick_email_source(profiles)
        assert result is not None
        assert result.name == "A"

    def test_empty_iterable_returns_none(self) -> None:
        assert pick_email_source([]) is None


class TestPropagateEmailToUnconfigured:
    """``propagate_email_to_unconfigured(source, others)`` copies
    ``source.email`` into every ``other.email`` that is unconfigured.
    """

    def test_unconfigured_source_propagates_nothing(self) -> None:
        """No email to share → no-op."""
        source = _profile("A")  # empty email
        others = [_profile("B"), _profile("C")]
        mutated = propagate_email_to_unconfigured(source, others)
        assert mutated == []
        assert all(is_email_unconfigured(p.email) for p in others)

    def test_propagates_full_email_config_to_all_unconfigured(self) -> None:
        """Every field of ``EmailConfig`` flows through, not just username."""
        source = _profile("A", _filled_email())
        b = _profile("B")
        c = _profile("C")
        mutated = propagate_email_to_unconfigured(source, [b, c])
        assert {p.name for p in mutated} == {"B", "C"}
        for p in (b, c):
            assert p.email.username == "alice@example.io"
            assert p.email.smtp_host == "smtp.example.io"
            assert p.email.smtp_port == 587
            assert p.email.use_tls is True
            assert p.email.password == "secret"
            assert p.email.enabled is True
            assert p.email.send_on_success is True
            assert p.email.send_on_failure is True

    def test_skips_already_configured_profiles(self) -> None:
        source = _profile("A", _filled_email("alice@example.io"))
        configured = _profile("B", _filled_email("bob@example.io"))
        unconfigured = _profile("C")
        mutated = propagate_email_to_unconfigured(
            source, [configured, unconfigured]
        )
        # Only C was touched
        assert mutated == [unconfigured]
        # B kept its own email
        assert configured.email.username == "bob@example.io"
        # C got the source's email
        assert unconfigured.email.username == "alice@example.io"

    def test_excludes_source_profile_by_id(self) -> None:
        """Even if the source is in the ``other_profiles`` iterable
        (caller forgot to filter), it must NOT be re-copied to itself.
        """
        source = _profile("A", _filled_email())
        original_email = source.email
        mutated = propagate_email_to_unconfigured(source, [source])
        assert mutated == []
        assert source.email is original_email  # identity preserved

    def test_propagated_emails_are_deep_copies(self) -> None:
        """Editing ``source.email`` after propagation leaves copies alone."""
        source = _profile("A", _filled_email())
        target = _profile("B")
        propagate_email_to_unconfigured(source, [target])
        # Mutate the source email after propagation
        source.email.smtp_host = "modified.example.io"
        source.email.username = "modified@example.io"
        # The target's copy is unaffected
        assert target.email.smtp_host == "smtp.example.io"
        assert target.email.username == "alice@example.io"

    def test_returns_mutated_profiles_in_iteration_order(self) -> None:
        source = _profile("A", _filled_email("alice@example.io"))
        p2 = _profile("B")
        p3 = _profile("C", _filled_email("carol@example.io"))
        p4 = _profile("D")
        mutated = propagate_email_to_unconfigured(source, [p2, p3, p4])
        # p3 already configured → skipped; p2 and p4 are propagated
        # in their original iteration order.
        assert [p.name for p in mutated] == ["B", "D"]

    def test_empty_other_profiles_returns_empty_list(self) -> None:
        source = _profile("A", _filled_email())
        assert propagate_email_to_unconfigured(source, []) == []
