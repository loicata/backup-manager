"""Tests for src.security.secure_memory."""

import pytest

from src.security.secure_memory import SecurePassword, secure_clear


class TestSecureClear:
    def test_clears_bytearray(self):
        data = bytearray(b"secret_password")
        secure_clear(data)
        assert all(b == 0 for b in data)

    def test_empty_bytearray(self):
        data = bytearray(b"")
        secure_clear(data)  # Should not raise
        assert len(data) == 0

    def test_none_input(self):
        secure_clear(None)  # Should not raise

    def test_string_does_not_raise(self):
        # Best-effort for str: should not crash
        secure_clear("password")

    def test_bytes_does_not_raise(self):
        # Best-effort for bytes: should not crash
        secure_clear(b"password")

    def test_large_bytearray(self):
        data = bytearray(b"x" * 10000)
        secure_clear(data)
        assert all(b == 0 for b in data)


class TestSecurePassword:
    """Tests for the SecurePassword context manager."""

    def test_get_returns_password(self):
        """get() returns the original password."""
        pw = SecurePassword("my_secret")
        assert pw.get() == "my_secret"

    def test_clear_zeros_buffer(self):
        """clear() zeros the internal bytearray."""
        pw = SecurePassword("secret")
        pw.clear()
        assert all(b == 0 for b in pw._buf)

    def test_get_after_clear_raises(self):
        """get() raises RuntimeError after clear()."""
        pw = SecurePassword("secret")
        pw.clear()
        with pytest.raises(RuntimeError, match="cleared"):
            pw.get()

    def test_context_manager_clears(self):
        """Exiting the context manager clears the password."""
        with SecurePassword("secret") as pw:
            assert pw.get() == "secret"
        assert pw._cleared is True
        with pytest.raises(RuntimeError):
            pw.get()

    def test_context_manager_clears_on_exception(self):
        """Password is cleared even if an exception occurs."""
        with pytest.raises(ValueError), SecurePassword("secret") as pw:
            raise ValueError("test error")
        assert pw._cleared is True

    def test_double_clear_safe(self):
        """Calling clear() twice does not raise."""
        pw = SecurePassword("secret")
        pw.clear()
        pw.clear()  # Should not raise

    def test_bool_true_when_valid(self):
        """bool() is True for a non-empty, non-cleared password."""
        pw = SecurePassword("secret")
        assert bool(pw) is True

    def test_bool_false_when_empty(self):
        """bool() is False for an empty password."""
        pw = SecurePassword("")
        assert bool(pw) is False

    def test_bool_false_when_cleared(self):
        """bool() is False after clear()."""
        pw = SecurePassword("secret")
        pw.clear()
        assert bool(pw) is False

    def test_repr_hides_password(self):
        """repr() does not leak the actual password."""
        pw = SecurePassword("super_secret_123")
        assert "super_secret_123" not in repr(pw)
        assert "***" in repr(pw)

    def test_repr_after_clear(self):
        """repr() shows 'cleared' after clear()."""
        pw = SecurePassword("secret")
        pw.clear()
        assert "cleared" in repr(pw)

    def test_unicode_password(self):
        """Non-ASCII passwords round-trip correctly."""
        pw = SecurePassword("p@$$wörd_日本語")
        assert pw.get() == "p@$$wörd_日本語"

    def test_type_error_on_non_string(self):
        """Constructor rejects non-string input."""
        with pytest.raises(TypeError, match="str"):
            SecurePassword(12345)  # type: ignore[arg-type]

    def test_del_clears_buffer(self):
        """__del__ clears the buffer as a fallback."""
        pw = SecurePassword("secret")
        buf_ref = pw._buf
        pw.__del__()
        assert all(b == 0 for b in buf_ref)
