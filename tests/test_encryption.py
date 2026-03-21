"""Tests for src.security.encryption."""

import os
from unittest.mock import patch

import pytest

from src.security.encryption import (
    KEY_SIZE,
    SALT_SIZE,
    decrypt_bytes,
    decrypt_file,
    derive_key,
    encrypt_bytes,
    encrypt_file,
    evaluate_password,
    retrieve_password,
    store_password,
)


class TestDeriveKey:
    def test_returns_correct_length(self):
        salt = os.urandom(SALT_SIZE)
        key = derive_key("password", salt)
        assert len(key) == KEY_SIZE

    def test_same_inputs_same_output(self):
        salt = os.urandom(SALT_SIZE)
        k1 = derive_key("password", salt)
        k2 = derive_key("password", salt)
        assert k1 == k2

    def test_different_passwords_different_keys(self):
        salt = os.urandom(SALT_SIZE)
        k1 = derive_key("password1", salt)
        k2 = derive_key("password2", salt)
        assert k1 != k2

    def test_different_salts_different_keys(self):
        s1 = os.urandom(SALT_SIZE)
        s2 = os.urandom(SALT_SIZE)
        k1 = derive_key("password", s1)
        k2 = derive_key("password", s2)
        assert k1 != k2


class TestEncryptDecryptBytes:
    def test_roundtrip(self):
        data = b"Hello, World!"
        encrypted = encrypt_bytes(data, "mypassword")
        decrypted = decrypt_bytes(encrypted, "mypassword")
        assert decrypted == data

    def test_encrypted_differs_from_plaintext(self):
        data = b"Secret data"
        encrypted = encrypt_bytes(data, "password")
        assert encrypted != data

    def test_different_encryptions_differ(self):
        data = b"Same data"
        e1 = encrypt_bytes(data, "password")
        e2 = encrypt_bytes(data, "password")
        assert e1 != e2  # Different salt/nonce each time

    def test_wrong_password_raises(self):
        data = b"Secret"
        encrypted = encrypt_bytes(data, "correct")
        with pytest.raises(Exception):
            decrypt_bytes(encrypted, "wrong")

    def test_truncated_data_raises(self):
        with pytest.raises(ValueError, match="too short"):
            decrypt_bytes(b"short", "password")

    def test_empty_data(self):
        encrypted = encrypt_bytes(b"", "password")
        decrypted = decrypt_bytes(encrypted, "password")
        assert decrypted == b""

    def test_large_data(self):
        data = os.urandom(10 * 1024 * 1024)  # 10 MB
        encrypted = encrypt_bytes(data, "password")
        decrypted = decrypt_bytes(encrypted, "password")
        assert decrypted == data


class TestEncryptDecryptFile:
    def test_roundtrip(self, tmp_path):
        src = tmp_path / "plain.txt"
        enc = tmp_path / "encrypted.wbenc"
        dec = tmp_path / "decrypted.txt"

        src.write_text("File content", encoding="utf-8")
        assert encrypt_file(src, enc, "password") is True
        assert enc.exists()
        assert encrypt_file(src, enc, "password") is True

        assert decrypt_file(enc, dec, "password") is True
        assert dec.read_text(encoding="utf-8") == "File content"

    def test_wrong_password_returns_false(self, tmp_path):
        src = tmp_path / "plain.txt"
        enc = tmp_path / "encrypted.wbenc"
        dec = tmp_path / "decrypted.txt"

        src.write_text("Secret", encoding="utf-8")
        encrypt_file(src, enc, "correct")
        assert decrypt_file(enc, dec, "wrong") is False

    def test_missing_file_returns_false(self, tmp_path):
        enc = tmp_path / "encrypted.wbenc"
        result = encrypt_file(tmp_path / "missing.txt", enc, "password")
        assert result is False


class TestPasswordStorage:
    def test_aes_roundtrip(self):
        """Test AES fallback (non-Windows or DPAPI unavailable)."""
        with patch("src.security.encryption._has_dpapi", return_value=False):
            stored = store_password("my_secret_password")
            assert stored.startswith("aes:")
            retrieved = retrieve_password(stored)
            assert retrieved == "my_secret_password"

    def test_legacy_plaintext_passthrough(self):
        assert retrieve_password("plaintext_value") == "plaintext_value"

    def test_invalid_aes_format_raises(self):
        with pytest.raises(ValueError):
            retrieve_password("aes:invalid")


class TestEvaluatePassword:
    def test_short_password(self):
        assert "too short" in evaluate_password("abc")

    def test_below_sixteen(self):
        assert "too short" in evaluate_password("Pass1234!")

    def test_exactly_sixteen(self):
        assert evaluate_password("1234567890123456") == ""

    def test_strong_password(self):
        assert evaluate_password("MyStr0ng!Pass#2026") == ""
