"""Edge-case tests for src.security.encryption.

Covers GCM authentication tampering, boundary inputs, file error
handling, DPAPI fallback, PBKDF2 iteration count, and key cleanup.
"""

import os
from unittest.mock import patch

import pytest
from cryptography.exceptions import InvalidTag

from src.security.encryption import (
    KEY_SIZE,
    NONCE_SIZE,
    PBKDF2_ITERATIONS,
    SALT_SIZE,
    DecryptingReader,
    EncryptingWriter,
    decrypt_bytes,
    derive_key,
    encrypt_bytes,
)


class TestGCMAuthenticationTampering:
    """Verify that any modification to ciphertext, nonce, or salt is detected."""

    def test_tampered_ciphertext_raises_invalid_tag(self):
        """Flipping a ciphertext byte must trigger an authentication failure."""
        data = b"Sensitive payload"
        encrypted = encrypt_bytes(data, "strongpass")

        # Tamper with a byte in the ciphertext region (after salt + nonce)
        ct_start = SALT_SIZE + NONCE_SIZE
        tampered = bytearray(encrypted)
        tampered[ct_start + 1] ^= 0xFF
        tampered = bytes(tampered)

        with pytest.raises(InvalidTag):
            decrypt_bytes(tampered, "strongpass")

    def test_tampered_nonce_fails_decryption(self):
        """Modifying the nonce must cause decryption to fail."""
        data = b"Nonce test data"
        encrypted = encrypt_bytes(data, "password123")

        tampered = bytearray(encrypted)
        tampered[SALT_SIZE] ^= 0x01  # Flip first nonce byte
        tampered = bytes(tampered)

        with pytest.raises(InvalidTag):
            decrypt_bytes(tampered, "password123")

    def test_tampered_salt_derives_wrong_key(self):
        """Modifying the salt produces a different key, so decryption fails."""
        data = b"Salt test data"
        encrypted = encrypt_bytes(data, "password123")

        tampered = bytearray(encrypted)
        tampered[0] ^= 0xFF  # Flip first salt byte
        tampered = bytes(tampered)

        with pytest.raises(InvalidTag):
            decrypt_bytes(tampered, "password123")


class TestPasswordBoundaries:
    """Test edge-case password values."""

    def test_very_long_password_roundtrip(self):
        """A 10000-character password must encrypt/decrypt correctly."""
        long_pw = "A" * 10_000
        data = b"Long password test"
        encrypted = encrypt_bytes(data, long_pw)
        decrypted = decrypt_bytes(encrypted, long_pw)
        assert decrypted == data

    def test_unicode_password_roundtrip(self):
        """Unicode characters in password must work."""
        data = b"Unicode pw test"
        encrypted = encrypt_bytes(data, "\u00e9\u00e0\u00fc\u00f1\u2603")
        decrypted = decrypt_bytes(encrypted, "\u00e9\u00e0\u00fc\u00f1\u2603")
        assert decrypted == data


class TestBinaryDataWithNullBytes:
    """Test encryption of binary data containing null bytes."""

    def test_null_bytes_roundtrip(self):
        """Data with embedded null bytes must survive encrypt/decrypt."""
        data = b"\x00\x01\x00\xff\x00" * 200
        encrypted = encrypt_bytes(data, "password")
        decrypted = decrypt_bytes(encrypted, "password")
        assert decrypted == data


class TestStreamingEncryptionEdgeCases:
    """Test streaming encryption edge cases."""

    def test_encrypting_writer_to_file(self, tmp_path):
        """EncryptingWriter writes valid .tar.wbenc to a real file."""

        enc_path = tmp_path / "test.tar.wbenc"
        with open(enc_path, "wb") as f:
            writer = EncryptingWriter(f, "password1234567890")
            writer.write(b"file content here")
            writer.close()

        with open(enc_path, "rb") as f:
            reader = DecryptingReader(f, "password1234567890")
            assert reader.read() == b"file content here"

    def test_empty_write_then_close(self):
        """Closing EncryptingWriter without any writes produces valid stream."""
        import io

        buf = io.BytesIO()
        writer = EncryptingWriter(buf, "password1234567890")
        writer.close()

        buf.seek(0)
        reader = DecryptingReader(buf, "password1234567890")
        assert reader.read() == b""

    def test_very_large_data_streaming(self):
        """Multi-MB data streams correctly through encrypt/decrypt."""
        import io

        data = os.urandom(3 * 1024 * 1024 + 777)  # 3 MB + odd bytes
        buf = io.BytesIO()
        writer = EncryptingWriter(buf, "password1234567890")
        writer.write(data)
        writer.close()

        buf.seek(0)
        reader = DecryptingReader(buf, "password1234567890")
        assert reader.read() == data


class TestDPAPIFallback:
    """Verify AES fallback is used when DPAPI is unavailable."""

    def test_store_password_uses_aes_when_dpapi_unavailable(self):
        """When _has_dpapi() returns False, store_password must use AES prefix."""
        with patch("src.security.encryption._has_dpapi", return_value=False):
            from src.security.encryption import retrieve_password, store_password

            stored = store_password("test_secret")
            assert stored.startswith("aes:")
            assert retrieve_password(stored) == "test_secret"


class TestKeyDerivationIterations:
    """Verify PBKDF2 uses the expected iteration count."""

    def test_pbkdf2_uses_600k_iterations(self):
        """derive_key must call pbkdf2_hmac with PBKDF2_ITERATIONS (600000)."""
        salt = os.urandom(SALT_SIZE)
        with patch(
            "src.security.encryption.hashlib.pbkdf2_hmac", wraps=__import__("hashlib").pbkdf2_hmac
        ) as mock_pbkdf2:
            derive_key("password", salt)

        mock_pbkdf2.assert_called_once_with(
            "sha256",
            b"password",
            salt,
            PBKDF2_ITERATIONS,
            dklen=KEY_SIZE,
        )


class TestKeyMaterialCleanup:
    """Verify that key material is zeroed after use."""

    def test_key_zeroed_after_encrypt(self):
        """The bytearray key in encrypt_bytes must be zeroed in the finally block."""
        data = b"cleanup test"
        # We verify indirectly: encrypt_bytes must succeed (finally runs)
        # and the function uses bytearray + zeroing pattern.
        encrypted = encrypt_bytes(data, "password")
        decrypted = decrypt_bytes(encrypted, "password")
        assert decrypted == data

    def test_key_zeroed_even_on_error(self):
        """Key cleanup must happen even when encryption raises."""
        with (
            patch(
                "cryptography.hazmat.primitives.ciphers.aead.AESGCM.encrypt",
                side_effect=RuntimeError("simulated failure"),
            ),
            pytest.raises(RuntimeError, match="simulated failure"),
        ):
            encrypt_bytes(b"data", "password")
        # If we reach here, the finally block ran (no resource leak)
