"""Tests for streaming tar encryption primitives.

Verifies round-trip encryption/decryption, error detection,
and tarfile compatibility of the .tar.wbenc format.
"""

import io
import os
import tarfile

import cryptography.exceptions
import pytest

from src.security.encryption import (
    CHUNK_SIZE,
    NONCE_SIZE,
    TAR_WBENC_HEADER_SIZE,
    TAR_WBENC_MAGIC,
    TAR_WBENC_VERSION,
    DecryptingReader,
    EncryptingWriter,
    StreamDecryptor,
    StreamEncryptor,
)

PASSWORD = "test-password-at-least-16"


class TestStreamEncryptorDecryptor:
    """Low-level StreamEncryptor / StreamDecryptor round-trip."""

    def test_single_chunk_round_trip(self):
        """Encrypt one chunk, decrypt it, verify match."""
        enc = StreamEncryptor(PASSWORD)
        header = enc.header()
        ct = enc.encrypt_chunk(b"Hello, World!")
        eof = enc.finalize()

        stream = io.BytesIO(header + ct + eof)
        dec = StreamDecryptor(PASSWORD)
        dec.read_header(stream)
        plaintext = dec.decrypt_next_chunk(stream)
        assert plaintext == b"Hello, World!"
        assert dec.decrypt_next_chunk(stream) is None  # EOF

    def test_multi_chunk_round_trip(self):
        """Multiple chunks decrypt in order."""
        enc = StreamEncryptor(PASSWORD)
        parts = [b"chunk-one", b"chunk-two", b"chunk-three"]
        data = enc.header()
        for p in parts:
            data += enc.encrypt_chunk(p)
        data += enc.finalize()

        stream = io.BytesIO(data)
        dec = StreamDecryptor(PASSWORD)
        dec.read_header(stream)
        for expected in parts:
            assert dec.decrypt_next_chunk(stream) == expected
        assert dec.decrypt_next_chunk(stream) is None

    def test_exact_chunk_size_boundary(self):
        """Chunk of exactly CHUNK_SIZE bytes works."""
        payload = b"X" * CHUNK_SIZE
        enc = StreamEncryptor(PASSWORD)
        data = enc.header() + enc.encrypt_chunk(payload) + enc.finalize()

        stream = io.BytesIO(data)
        dec = StreamDecryptor(PASSWORD)
        dec.read_header(stream)
        assert dec.decrypt_next_chunk(stream) == payload
        assert dec.decrypt_next_chunk(stream) is None

    def test_wrong_password_raises(self):
        """Decryption with wrong password raises on first chunk."""
        enc = StreamEncryptor(PASSWORD)
        data = enc.header() + enc.encrypt_chunk(b"secret") + enc.finalize()

        stream = io.BytesIO(data)
        dec = StreamDecryptor("wrong-password-1234567")
        dec.read_header(stream)
        with pytest.raises(
            (ValueError, cryptography.exceptions.InvalidTag),
        ):
            dec.decrypt_next_chunk(stream)

    def test_tampered_ciphertext_raises(self):
        """Flipping a byte in ciphertext causes auth failure."""
        enc = StreamEncryptor(PASSWORD)
        data = bytearray(enc.header() + enc.encrypt_chunk(b"data") + enc.finalize())

        # Tamper with a byte inside the ciphertext (after header + length + nonce)
        ct_offset = TAR_WBENC_HEADER_SIZE + 4 + NONCE_SIZE + 2
        data[ct_offset] ^= 0xFF

        stream = io.BytesIO(bytes(data))
        dec = StreamDecryptor(PASSWORD)
        dec.read_header(stream)
        with pytest.raises(
            (ValueError, cryptography.exceptions.InvalidTag),
        ):
            dec.decrypt_next_chunk(stream)

    def test_truncated_stream_raises(self):
        """Truncated stream raises ValueError."""
        enc = StreamEncryptor(PASSWORD)
        full = enc.header() + enc.encrypt_chunk(b"data") + enc.finalize()
        truncated = full[: TAR_WBENC_HEADER_SIZE + 10]  # Cut mid-chunk

        stream = io.BytesIO(truncated)
        dec = StreamDecryptor(PASSWORD)
        dec.read_header(stream)
        with pytest.raises(ValueError, match="Unexpected end"):
            dec.decrypt_next_chunk(stream)

    def test_bad_magic_raises(self):
        """Invalid magic bytes raise ValueError."""
        data = b"BAAD" + bytes([TAR_WBENC_VERSION]) + b"\x00" * 32
        stream = io.BytesIO(data)
        dec = StreamDecryptor(PASSWORD)
        with pytest.raises(ValueError, match="bad magic"):
            dec.read_header(stream)

    def test_bad_version_raises(self):
        """Unsupported version raises ValueError."""
        data = TAR_WBENC_MAGIC + bytes([99]) + b"\x00" * 32
        stream = io.BytesIO(data)
        dec = StreamDecryptor(PASSWORD)
        with pytest.raises(ValueError, match="Unsupported"):
            dec.read_header(stream)

    def test_header_format(self):
        """Header has correct structure."""
        enc = StreamEncryptor(PASSWORD)
        hdr = enc.header()
        assert len(hdr) == TAR_WBENC_HEADER_SIZE
        assert hdr[:4] == TAR_WBENC_MAGIC
        assert hdr[4] == TAR_WBENC_VERSION
        assert hdr[21:37] == b"\x00" * 16  # Reserved

    def test_empty_chunk_raises(self):
        """Encrypting empty bytes raises ValueError."""
        enc = StreamEncryptor(PASSWORD)
        with pytest.raises(ValueError, match="empty"):
            enc.encrypt_chunk(b"")

    def test_sequential_nonces(self):
        """Nonces increment sequentially."""
        enc = StreamEncryptor(PASSWORD)
        ct1 = enc.encrypt_chunk(b"a")
        ct2 = enc.encrypt_chunk(b"b")
        # Nonce is bytes 4..16 in each chunk (after 4B length prefix)
        nonce1 = ct1[4 : 4 + NONCE_SIZE]
        nonce2 = ct2[4 : 4 + NONCE_SIZE]
        assert int.from_bytes(nonce1, "big") == 0
        assert int.from_bytes(nonce2, "big") == 1


class TestEncryptingWriterDecryptingReader:
    """High-level writer/reader round-trip."""

    def test_small_data_round_trip(self):
        """Write small data, read it back."""
        buf = io.BytesIO()
        writer = EncryptingWriter(buf, PASSWORD)
        writer.write(b"hello world")
        writer.close()

        buf.seek(0)
        reader = DecryptingReader(buf, PASSWORD)
        assert reader.read() == b"hello world"

    def test_close_is_idempotent(self):
        """Calling close() twice must not raise or double-write."""
        buf = io.BytesIO()
        writer = EncryptingWriter(buf, PASSWORD)
        writer.write(b"once")
        writer.close()
        size_after_first_close = len(buf.getvalue())

        writer.close()  # Must be a no-op
        assert len(buf.getvalue()) == size_after_first_close

    def test_close_tolerates_already_closed_dest(self):
        """GC-finalised writer with closed dest must not raise.

        Reproduces the scenario where an exception mid-archive causes
        the enclosing ``with open(...)`` to close the file before the
        garbage collector gets to finalise the EncryptingWriter.  The
        writer's close() used to raise ``ValueError: write to closed
        file`` at interpreter shutdown; it now swallows the write.
        """
        buf = io.BytesIO()
        writer = EncryptingWriter(buf, PASSWORD)
        writer.write(b"partial")
        buf.close()  # Destination closed before writer.close()
        # Must not raise — mimics GC order during an interrupted write.
        writer.close()
        assert writer._closed is True

    def test_close_propagates_write_error_on_open_dest(self):
        """A genuine OSError on an open dest must NOT be swallowed.

        If the destination is still open but writing fails (disk full,
        broken pipe, network drop), close() must surface the error so
        the caller aborts the backup and discards the partial archive.
        Swallowing here would promote a truncated .tar.wbenc to its
        final name and later delete the source material.
        """

        class _BrokenBuf(io.BytesIO):
            def write(self, data):  # type: ignore[override]
                if self._broken:
                    raise OSError("pipe broken")
                return super().write(data)

            def flush(self):  # type: ignore[override]
                if self._broken:
                    raise OSError("pipe broken")
                return super().flush()

            _broken = False

        buf = _BrokenBuf()
        writer = EncryptingWriter(buf, PASSWORD)
        writer.write(b"data")
        buf._broken = True
        with pytest.raises(OSError, match="pipe broken"):
            writer.close()

    def test_multi_chunk_data(self):
        """Data spanning multiple chunks decrypts correctly."""
        payload = b"A" * (CHUNK_SIZE * 2 + 500)
        buf = io.BytesIO()
        writer = EncryptingWriter(buf, PASSWORD)
        writer.write(payload)
        writer.close()

        buf.seek(0)
        reader = DecryptingReader(buf, PASSWORD)
        assert reader.read() == payload

    def test_incremental_writes(self):
        """Many small writes produce correct output."""
        buf = io.BytesIO()
        writer = EncryptingWriter(buf, PASSWORD)
        for i in range(1000):
            writer.write(f"line-{i}\n".encode())
        writer.close()

        expected = b"".join(f"line-{i}\n".encode() for i in range(1000))
        buf.seek(0)
        reader = DecryptingReader(buf, PASSWORD)
        assert reader.read() == expected

    def test_incremental_reads(self):
        """Reading in small chunks works correctly."""
        payload = b"0123456789" * 100
        buf = io.BytesIO()
        writer = EncryptingWriter(buf, PASSWORD)
        writer.write(payload)
        writer.close()

        buf.seek(0)
        reader = DecryptingReader(buf, PASSWORD)
        result = bytearray()
        while True:
            chunk = reader.read(7)  # Odd size to test boundary
            if not chunk:
                break
            result.extend(chunk)
        assert bytes(result) == payload


class TestTarfileIntegration:
    """EncryptingWriter + DecryptingReader through tarfile."""

    def test_tar_round_trip_single_file(self):
        """Create tar with one file, encrypt, decrypt, extract."""
        buf = io.BytesIO()
        writer = EncryptingWriter(buf, PASSWORD)

        with tarfile.open(fileobj=writer, mode="w|") as tar:
            data = b"file content here"
            info = tarfile.TarInfo(name="test.txt")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

        writer.close()

        buf.seek(0)
        reader = DecryptingReader(buf, PASSWORD)
        with tarfile.open(fileobj=reader, mode="r|") as tar:
            member = next(iter(tar))
            assert member.name == "test.txt"
            extracted = tar.extractfile(member)
            assert extracted.read() == b"file content here"

    def test_tar_round_trip_multiple_files(self):
        """Tar with multiple files and directories."""
        files = {
            "dir/file1.txt": b"content one",
            "dir/sub/file2.bin": b"\x00\x01\x02" * 1000,
            "readme.md": b"# Hello",
        }

        buf = io.BytesIO()
        writer = EncryptingWriter(buf, PASSWORD)

        with tarfile.open(fileobj=writer, mode="w|") as tar:
            for name, content in files.items():
                info = tarfile.TarInfo(name=name)
                info.size = len(content)
                tar.addfile(info, io.BytesIO(content))

        writer.close()

        buf.seek(0)
        reader = DecryptingReader(buf, PASSWORD)
        extracted = {}
        with tarfile.open(fileobj=reader, mode="r|") as tar:
            for member in tar:
                f = tar.extractfile(member)
                if f:
                    extracted[member.name] = f.read()

        assert extracted == files

    def test_tar_large_file(self):
        """Single large file spanning many chunks."""
        large_data = os.urandom(CHUNK_SIZE * 3 + 12345)

        buf = io.BytesIO()
        writer = EncryptingWriter(buf, PASSWORD)

        with tarfile.open(fileobj=writer, mode="w|") as tar:
            info = tarfile.TarInfo(name="big.bin")
            info.size = len(large_data)
            tar.addfile(info, io.BytesIO(large_data))

        writer.close()

        buf.seek(0)
        reader = DecryptingReader(buf, PASSWORD)
        with tarfile.open(fileobj=reader, mode="r|") as tar:
            member = next(iter(tar))
            assert tar.extractfile(member).read() == large_data

    def test_tar_wrong_password_fails_early(self):
        """Wrong password on tar extraction raises quickly."""
        buf = io.BytesIO()
        writer = EncryptingWriter(buf, PASSWORD)

        with tarfile.open(fileobj=writer, mode="w|") as tar:
            info = tarfile.TarInfo(name="secret.txt")
            data = b"secret data"
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

        writer.close()

        buf.seek(0)
        with pytest.raises(
            (ValueError, cryptography.exceptions.InvalidTag),
        ):
            reader = DecryptingReader(buf, "wrong-password-1234567")
            with tarfile.open(fileobj=reader, mode="r|"):
                pass
