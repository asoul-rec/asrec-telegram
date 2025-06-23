import io
import random
from unittest import TestCase

from src.ioutils.wrapped_fileio import PartedFile, CombinedFile


class TestPartedFile(TestCase):
    _content = None
    _content_size = 64 << 20  # 64 MB

    @classmethod
    def setUpClass(cls):
        """Generate a large block of random bytes once for all tests."""
        cls._content = random.randbytes(cls._content_size)

    def open_big_file(self):
        return io.BytesIO(self._content)

    def test_seek_and_tell(self):
        """Test if seek and tell work correctly relative to the part's boundaries."""
        test_offset, test_size = 10 << 20, 5 << 20  # 10 to 15 MB part of the big file

        with self.open_big_file() as big_file:
            parted_file = PartedFile(big_file, test_offset, test_size)

            # Initial state
            self.assertEqual(parted_file.tell(), 0)

            # Seek from start (SEEK_SET)
            parted_file.seek(100)
            self.assertEqual(parted_file.tell(), 100)

            # Seek from current (SEEK_CUR)
            parted_file.seek(50, io.SEEK_CUR)
            self.assertEqual(parted_file.tell(), 150)

            # Seek from end (SEEK_END)
            parted_file.seek(-200, io.SEEK_END)
            self.assertEqual(parted_file.tell(), test_size - 200)

            # Seeking past boundaries should still update tell()
            parted_file.seek(test_size + 1000)
            self.assertEqual(parted_file.tell(), test_size + 1000)

            # Seeking before the start should raise an error
            with self.assertRaises(OSError):
                parted_file.seek(-1)
            with self.assertRaises(OSError):
                parted_file.seek(-test_size - 1001, io.SEEK_CUR)
            with self.assertRaises(OSError):
                parted_file.seek(-test_size - 1, io.SEEK_END)

            # Parted file is in normal state and not modified after invalid seeks
            parted_file.seek(-1001, io.SEEK_CUR)
            self.assertEqual(parted_file.tell(), test_size - 1)

    def test_read(self):
        test_offset, test_size = 7 << 20, 6 << 20  # 7 to 13MB part of the big file

        with self.open_big_file() as big_file:
            parted_file = PartedFile(big_file, test_offset, test_size)

            # 1. Read from the beginning
            parted_file.seek(0)
            read_content = parted_file.read(1024)
            expected_content = self._content[test_offset: test_offset + 1024]
            self.assertEqual(read_content, expected_content)
            self.assertEqual(parted_file.tell(), 1024)

            # 2. Read from the middle
            parted_file.seek(1 << 20)  # Seek to 1MB into the part
            read_content = parted_file.read(512)
            start_pos = test_offset + (1 << 20)
            expected_content = self._content[start_pos: start_pos + 512]
            self.assertEqual(read_content, expected_content)
            self.assertEqual(parted_file.tell(), (1 << 20) + 512)

            # 3. Read everything left (-1)
            parted_file.seek(-2048, io.SEEK_END)  # Seek to 2KB before the end
            read_content = parted_file.read()
            start_pos = test_offset + test_size - 2048
            expected_content = self._content[start_pos: test_offset + test_size]
            self.assertEqual(read_content, expected_content)
            self.assertEqual(parted_file.tell(), test_size)

            # 4. Read past the end of the part should return empty bytes
            read_content = parted_file.read(100)
            self.assertEqual(read_content, b"")
            parted_file.seek(1024, io.SEEK_END)
            read_content = parted_file.read(100)
            self.assertEqual(read_content, b"")

    def test_close(self):
        # Test 1: Closing the part does not close the parent by default
        with self.open_big_file() as big_file:
            parted_file = PartedFile(big_file, 345, 678)
            parted_file.read(1)  # Read a byte to ensure the part is functional
            self.assertFalse(parted_file.closed)
            parted_file.close()
            self.assertTrue(parted_file.closed)
            self.assertFalse(big_file.closed)
            with self.assertRaises(ValueError):
                parted_file.read(1)
            with self.assertRaises(ValueError):
                parted_file.tell()
            with self.assertRaises(ValueError):
                parted_file.seek(0)
        self.assertTrue(big_file.closed)

        # Test 2: Closing the part closes the parent when requested
        with self.open_big_file() as big_file:
            parted_file = PartedFile(big_file, 0, 100, close_parent=True)
            parted_file.close()
            self.assertTrue(parted_file.closed)
            self.assertTrue(big_file.closed)

        # Test 3: Closing the parent also closes the part
        with self.open_big_file() as big_file:
            parted_file = PartedFile(big_file, 0, 100)
            self.assertFalse(parted_file.closed)
        self.assertTrue(parted_file.closed)


class TestCombinedFile(TestCase):
    _content = None
    _content_size = 10 * 1024 * 1024  # 10 MB

    @classmethod
    def setUpClass(cls):
        cls._content = random.randbytes(cls._content_size)

    def test_combination_and_read(self):
        """Test if the combined file reads correctly across part boundaries."""
        # Split the content into 3 parts
        part1_data = self._content[0: 3 * 1024 * 1024]
        part2_data = self._content[3 * 1024 * 1024: 7 * 1024 * 1024]
        part3_data = self._content[7 * 1024 * 1024:]

        part1 = io.BytesIO(part1_data)
        part2 = io.BytesIO(part2_data)
        part3 = io.BytesIO(part3_data)
        empty_part = [io.BytesIO()]

        combined_file = CombinedFile([
            *(empty_part * 13), part1, *(empty_part * 19), part2, *(empty_part * 7), part3, *(empty_part * 16)
        ])

        # Check total size
        self.assertEqual(combined_file.size, self._content_size)

        # 1. Read the whole file and verify
        combined_file.seek(0)
        read_content = combined_file.read()
        self.assertEqual(read_content, self._content)

        # 2. Read across the first boundary
        offset = len(part1_data) - 512
        size = 1024
        combined_file.seek(offset)
        read_content = combined_file.read(size)
        expected_content = self._content[offset: offset + size]
        self.assertEqual(read_content, expected_content)

        # 3. Read across the second boundary
        offset = len(part1_data) + len(part2_data) - 256
        size = 2048
        combined_file.seek(offset)
        read_content = combined_file.read(size)
        expected_content = self._content[offset: offset + size]
        self.assertEqual(read_content, expected_content)
        self.assertEqual(combined_file.tell(), offset + len(expected_content))

        # 4. Short read is allowed
        combined_file.seek(-512, io.SEEK_END)
        read_content = combined_file.read(1024)
        expected_content = self._content[-512:]
        self.assertEqual(read_content, expected_content)
        self.assertEqual(combined_file.tell(), combined_file.size)

    def test_seek_and_tell(self):
        """Test seeking and telling in the combined file."""
        parts = [io.BytesIO(b'A' * 100), io.BytesIO(b'B' * 200), io.BytesIO(b'C' * 150)]
        combined_file = CombinedFile(parts)

        self.assertEqual(combined_file.size, 450)
        self.assertEqual(combined_file.tell(), 0)

        # Seek inside part 2
        combined_file.seek(150)
        self.assertEqual(combined_file.tell(), 150)
        self.assertEqual(combined_file.read(1), b'B')

        # Seek from end
        combined_file.seek(-100, io.SEEK_END)
        self.assertEqual(combined_file.read(1), b'C')
        self.assertEqual(combined_file.tell(), 351)

        # Seek from current
        combined_file.seek(19, io.SEEK_CUR)
        self.assertEqual(combined_file.tell(), 370)

    def test_close(self):
        """Test that closing CombinedFile closes all its parts."""
        part1 = io.BytesIO(b'data1')
        part2 = io.BytesIO(b'data2')
        with CombinedFile([part1, part2], close_parts=True) as combined_file:
            self.assertFalse(combined_file.closed)
            self.assertFalse(part1.closed)
            self.assertFalse(part2.closed)
        self.assertTrue(combined_file.closed)
        self.assertTrue(part1.closed)
        self.assertTrue(part2.closed)

        part1 = io.BytesIO(b'data1')
        part2 = io.BytesIO(b'data2')
        with CombinedFile([part1, part2], close_parts=False) as combined_file:
            self.assertFalse(combined_file.closed)
            self.assertFalse(part1.closed)
            self.assertFalse(part2.closed)
        self.assertTrue(combined_file.closed)
        self.assertFalse(part1.closed)
        self.assertFalse(part2.closed)
