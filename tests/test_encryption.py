"""Tests for src.security.encryption."""

import os
from unittest.mock import patch

import pytest
from cryptography.exceptions import InvalidTag

from src.security.encryption import (
    KEY_SIZE,
    SALT_SIZE,
    DecryptingReader,
    EncryptingWriter,
    decrypt_bytes,
    derive_key,
    encrypt_bytes,
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

    def test_empty_password_rejected(self):
        """Empty passwords must raise: they derive a deterministic key
        from the salt alone — a meaningless 'encryption' that decrypts
        with any empty password on any machine."""
        salt = os.urandom(SALT_SIZE)
        with pytest.raises(ValueError, match="empty"):
            derive_key("", salt)

    def test_unicode_password_normalised_nfc(self):
        """Visually identical passwords must derive the same key.

        ``é`` as a single codepoint (U+00E9, precomposed, NFC) and
        ``é`` as ``e`` + ``U+0301`` (combining acute, NFD) look the
        same but encode to different byte sequences. Without NFC
        normalisation a user whose IME sometimes emits NFD would lose
        access to a backup encrypted when the IME emitted NFC.
        """
        salt = os.urandom(SALT_SIZE)
        precomposed = derive_key("caf\u00e9", salt)  # "café" NFC
        combining = derive_key("cafe\u0301", salt)  # "café" NFD
        assert precomposed == combining


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
        with pytest.raises(InvalidTag):
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


class TestStreamingEncryptDecrypt:
    """Test EncryptingWriter / DecryptingReader round-trip."""

    def test_roundtrip(self):
        import io

        buf = io.BytesIO()
        writer = EncryptingWriter(buf, "password1234567890")
        writer.write(b"File content")
        writer.close()

        buf.seek(0)
        reader = DecryptingReader(buf, "password1234567890")
        assert reader.read() == b"File content"

    def test_wrong_password_raises(self):
        import io

        buf = io.BytesIO()
        writer = EncryptingWriter(buf, "correct-password-1234")
        writer.write(b"Secret")
        writer.close()

        buf.seek(0)
        from cryptography.exceptions import InvalidTag

        with pytest.raises((ValueError, InvalidTag)):
            reader = DecryptingReader(buf, "wrong-password-12345678")
            reader.read()


class TestPasswordStorage:
    def test_aes_roundtrip(self):
        """Test AES fallback (non-Windows or DPAPI unavailable)."""
        with patch("src.security.encryption._has_dpapi", return_value=False):
            stored = store_password("my_secret_password")
            assert stored.startswith("aes:")
            retrieved = retrieve_password(stored)
            assert retrieved == "my_secret_password"

    def test_unprefixed_payload_rejected(self):
        """Unprefixed payloads are a downgrade-attack surface and must
        raise. Previously they were silently returned as plaintext,
        letting anyone who could edit the profile file plant an
        arbitrary 'password'."""
        with pytest.raises(ValueError, match="format prefix"):
            retrieve_password("plaintext_value")

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
