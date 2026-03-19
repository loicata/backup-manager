"""Tests for src.security.secure_memory."""

from src.security.secure_memory import secure_clear


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
