"""
Tests for src.security.encryption — password evaluation, password store/retrieve
round-trip (b64 fallback), and encrypt/decrypt round-trip if cryptography is available.
"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.security.encryption import (
    CryptoEngine,
    EncryptionConfig,
    EncryptionAlgorithm,
    evaluate_password,
    store_password,
    retrieve_password,
    get_crypto_engine,
)


class TestEvaluatePassword(unittest.TestCase):
    """Test password strength evaluation."""

    def test_empty_password(self):
        result = evaluate_password("")
        self.assertIn("16", result)
        self.assertTrue(len(result) > 0)

    def test_too_short(self):
        result = evaluate_password("abc")
        self.assertIn("13 more", result)

    def test_exactly_15_chars(self):
        result = evaluate_password("a" * 15)
        self.assertIn("1 more", result)

    def test_exactly_16_chars_valid(self):
        result = evaluate_password("a" * 16)
        self.assertEqual(result, "")

    def test_long_password_valid(self):
        result = evaluate_password("a" * 32)
        self.assertEqual(result, "")


class TestStoreRetrievePassword(unittest.TestCase):
    """Test password store/retrieve round-trip using b64 fallback."""

    def test_empty_password(self):
        self.assertEqual(store_password(""), "")
        self.assertEqual(retrieve_password(""), "")

    def test_b64_roundtrip(self):
        """Test store/retrieve round-trip (DPAPI on Windows, b64 fallback otherwise)."""
        stored = store_password("MySecretPassword123!")
        self.assertTrue(
            stored.startswith("dpapi:") or stored.startswith("b64:"),
            f"Unexpected stored format: {stored}"
        )
        retrieved = retrieve_password(stored)
        self.assertEqual(retrieved, "MySecretPassword123!")

    def test_b64_format_explicit(self):
        """Directly test b64 format retrieval."""
        import base64
        password = "TestPassword"
        b64_stored = "b64:" + base64.b64encode(password.encode("utf-8")).decode("ascii")
        self.assertEqual(retrieve_password(b64_stored), password)

    def test_retrieve_invalid_b64(self):
        result = retrieve_password("b64:!!!invalid!!!")
        self.assertEqual(result, "")

    def test_retrieve_invalid_dpapi(self):
        result = retrieve_password("dpapi:invalidhex")
        self.assertEqual(result, "")


class TestEncryptionConfig(unittest.TestCase):
    """Test EncryptionConfig dataclass defaults."""

    def test_defaults(self):
        ec = EncryptionConfig()
        self.assertFalse(ec.enabled)
        self.assertEqual(ec.algorithm, EncryptionAlgorithm.AES_256_GCM.value)
        self.assertEqual(ec.key_env_variable, "")
        self.assertEqual(ec.stored_password_b64, "")


class TestCryptoEngineEncryptDecrypt(unittest.TestCase):
    """Test encrypt/decrypt round-trip if the cryptography library is available."""

    def setUp(self):
        self.engine = CryptoEngine()

    @unittest.skipUnless(
        CryptoEngine()._detect_backend() == "cryptography",
        "cryptography library not installed"
    )
    def test_encrypt_decrypt_bytes_roundtrip(self):
        plaintext = b"Hello, this is sensitive backup data!"
        password = "StrongPassword!1234"
        encrypted = self.engine.encrypt_bytes(plaintext, password)
        self.assertNotEqual(encrypted, plaintext)
        decrypted = self.engine.decrypt_bytes(encrypted, password)
        self.assertEqual(decrypted, plaintext)

    @unittest.skipUnless(
        CryptoEngine()._detect_backend() == "cryptography",
        "cryptography library not installed"
    )
    def test_wrong_password_raises(self):
        plaintext = b"Secret data"
        encrypted = self.engine.encrypt_bytes(plaintext, "CorrectPassword!")
        with self.assertRaises(ValueError):
            self.engine.decrypt_bytes(encrypted, "WrongPassword!!")

    @unittest.skipUnless(
        CryptoEngine()._detect_backend() == "cryptography",
        "cryptography library not installed"
    )
    def test_encrypt_decrypt_file_roundtrip(self):
        plaintext = b"File content for encryption test."
        password = "FileTestPassword1234"
        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "source.txt"
            enc = Path(tmpdir) / "source.txt.wbenc"
            dec = Path(tmpdir) / "decrypted.txt"

            src.write_bytes(plaintext)
            self.assertTrue(self.engine.encrypt_file(src, enc, password))
            self.assertTrue(enc.exists())
            self.assertTrue(self.engine.decrypt_file(enc, dec, password))
            self.assertEqual(dec.read_bytes(), plaintext)

    @unittest.skipUnless(
        CryptoEngine()._detect_backend() == "cryptography",
        "cryptography library not installed"
    )
    def test_is_encrypted_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "plain.txt"
            enc = Path(tmpdir) / "plain.txt.wbenc"
            src.write_bytes(b"test data")
            self.engine.encrypt_file(src, enc, "Password12345678")
            self.assertTrue(CryptoEngine.is_encrypted_file(enc))
            self.assertFalse(CryptoEngine.is_encrypted_file(src))


if __name__ == "__main__":
    unittest.main()
