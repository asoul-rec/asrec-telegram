import random
from unittest import TestCase
from unittest.mock import MagicMock

from src.ioutils.cached_callback_fileio import CachedCustomFile


class TestCachedCustomFile(TestCase):
    _content = None
    _content_size = 5 << 20  # 5 MB

    @classmethod
    def setUpClass(cls):
        """Generate a large block of random bytes once for all tests."""
        cls._content = random.randbytes(cls._content_size)

    def _mock_reader(self, read_chunk_size: int):
        """
        Returns a mock reader that simulates reading aligned chunks.
        The reader may return a chunk with a different offset than requested,
        but it guarantees the requested offset is within the returned chunk.
        """

        def reader(offset: int) -> tuple[int, bytes]:
            # Simulate a reader that only reads from aligned block boundaries
            aligned_offset = (offset // read_chunk_size) * read_chunk_size
            chunk = self._content[aligned_offset:aligned_offset + read_chunk_size]
            return aligned_offset, chunk

        # Wrap the actual reader function in a Mock to track calls
        return MagicMock(side_effect=reader)

    def test_read_sequential(self):
        """Test sequential reading of the file."""
        mock_reader = self._mock_reader(read_chunk_size=1024)
        cached_file = CachedCustomFile(mock_reader, self._content_size)

        # Read the whole file sequentially
        read_content = cached_file.read()

        self.assertEqual(read_content, self._content)
        # Check that the reader was called multiple times
        self.assertGreater(mock_reader.call_count, 1)

    def test_read_with_cache_hits(self):
        """Test that the cache is being used by reading the same data multiple times."""
        mock_reader = self._mock_reader(read_chunk_size=4096)
        cached_file = CachedCustomFile(mock_reader, self._content_size)

        # First read - should call the reader
        cached_file.seek(100)
        cached_file.read(2048)
        first_call_count = mock_reader.call_count
        self.assertGreater(first_call_count, 0)

        # Second read of the same data - should hit the cache
        cached_file.seek(100)
        cached_file.read(2048)
        self.assertEqual(mock_reader.call_count, first_call_count, "Reader should not be called for a cached read")

        # Third read, partially overlapping - should also hit the cache
        cached_file.seek(200)
        cached_file.read(3000)
        self.assertEqual(mock_reader.call_count, first_call_count,
                         "Reader should not be called for an overlapping cached read")

    def test_seek_and_read(self):
        """Test seeking to different positions and then reading."""
        mock_reader = self._mock_reader(read_chunk_size=1024)
        cached_file = CachedCustomFile(mock_reader, self._content_size)

        # Seek to the middle and read
        offset = self._content_size // 2
        cached_file.seek(offset)
        read_content = cached_file.read(512)
        self.assertEqual(read_content, self._content[offset:offset + 512])
        self.assertEqual(cached_file.tell(), offset + 512)

        # Seek from the end and read
        cached_file.seek(-1024, 2)  # io.SEEK_END
        read_content = cached_file.read(256)
        offset_from_end = self._content_size - 1024
        self.assertEqual(read_content, self._content[offset_from_end:offset_from_end + 256])
        self.assertEqual(cached_file.tell(), offset_from_end + 256)

    def test_cache_eviction(self):
        """Test that the cache evicts the oldest entry when the buffer size is exceeded."""
        chunk_size = 1024
        # Buffer can hold exactly 3 chunks. Reading a 4th will cause eviction.
        buffer_size = 3 * chunk_size
        mock_reader = self._mock_reader(read_chunk_size=chunk_size)
        cached_file = CachedCustomFile(mock_reader, self._content_size, buffer_size=buffer_size)

        # 1. Read 4 chunks to trigger an eviction.
        cached_file.read(chunk_size * 4)

        # At this point, the cache has read chunks for offsets 0, 1024, 2048, 3072.
        # Since buffer size is 3*chunk_size, the first chunk (offset 0) should be evicted.
        # The cache should now contain chunks for offsets 1024, 2048, and 3072.
        self.assertEqual(len(cached_file._data_chunks), 3)
        self.assertNotIn(0, cached_file._offsets)
        self.assertIn(1024, cached_file._offsets)
        self.assertIn(2048, cached_file._offsets)
        self.assertIn(3072, cached_file._offsets)

        call_count_after_fill = mock_reader.call_count

        # 2. Seek back to the start and read the first chunk again.
        # This should result in a cache miss and a new reader call.
        cached_file.seek(0)
        cached_file.read(chunk_size)

        self.assertEqual(mock_reader.call_count, call_count_after_fill + 1,
                         "Reader should be called again for evicted data")

        # 3. Reading chunk 0 again should cause another eviction.
        # The oldest remaining chunk (offset 1024) should now be evicted.
        # The cache should contain chunks for offsets 2048, 3072, and 0.
        self.assertNotIn(1024, cached_file._offsets)
        self.assertIn(0, cached_file._offsets)
        self.assertIn(2048, cached_file._offsets)
        self.assertIn(3072, cached_file._offsets)