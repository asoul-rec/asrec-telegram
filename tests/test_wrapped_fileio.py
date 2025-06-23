from unittest import TestCase
from ioutils.wrapped_fileio import FilePart
import random
import io


class TestFilePart(TestCase):
    _content = None

    @classmethod
    def setUpClass(cls):
        cls._content = random.randbytes(64 << 20)

    def open_big_file(self):
        return io.BytesIO(self._content)

    def test_tell(self):
        with self.open_big_file() as big_file:
            file_part = FilePart(big_file, 1024, 1024)
            self.assertEqual(file_part.tell(), file_part._offset)
            file_part.seek(random.randint(0, 1023), io.SEEK_SET)
            self.assertEqual(file_part.tell(), file_part._offset)
            file_part.seek(-1024, io.SEEK_SET)
            self.assertEqual(file_part.tell(), file_part._offset)
            file_part.seek(1 << 40, io.SEEK_SET)
            self.assertEqual(file_part.tell(), file_part._offset)

    def test_seek(self):
        test_offset = 7 << 20
        test_size = 5 << 20
        with self.open_big_file() as big_file:
            file_part = FilePart(big_file, test_offset, test_size)
            self.assertEqual(file_part.tell(), 0)
            file_part.seek(789, io.SEEK_SET)
            self.assertEqual(file_part.tell(), 789)
            self.assertEqual(file_part._base_offset, test_offset + 789)
            file_part.seek(123, io.SEEK_CUR)
            self.assertEqual(file_part._offset, 789 + 123)
            self.assertEqual(file_part._base_offset, test_offset + 789 + 123)

    def test_seekable_readable(self):
        # Always true for FilePart
        with self.open_big_file() as big_file:
            file_part = FilePart(big_file, 1 << 40, 1 << 40)
            self.assertTrue(file_part.seekable())
            self.assertTrue(file_part.readable())
        self.assertTrue(file_part.seekable())
        self.assertTrue(file_part.readable())

    def test_read(self):
        self.fail()

    def test_close(self):
        with self.open_big_file() as big_file:
            # close file_part without closing parent
            file_part = FilePart(big_file, 1 << 40, 1 << 40)
            self.assertFalse(file_part.closed)
            file_part.close()
            self.assertTrue(file_part.closed)
            # context manager should close file_part
            with FilePart(big_file, 1 << 40, 1 << 40) as file_part:
                self.assertFalse(file_part.closed)
            self.assertTrue(file_part.closed)
            # parent should not be closed
            self.assertFalse(big_file.closed)
            # close file_part with closing parent
            file_part = FilePart(big_file, 1 << 40, 1 << 40, close_parent=True)
            self.assertFalse(file_part.closed)
            file_part.close()
            self.assertTrue(file_part.closed)
            self.assertTrue(big_file.closed)

        # file_part should be closed after parent is closed
        with self.open_big_file() as big_file:
            file_part = FilePart(big_file, 1 << 40, 1 << 40)
            self.assertFalse(file_part.closed)
        self.assertTrue(file_part.closed)
