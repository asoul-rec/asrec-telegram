from bisect import bisect_right
from collections.abc import Callable
from math import inf

from .wrapped_fileio import FileProxy


class CachedCustomFile(FileProxy):
    """
    A read-only, seekable file-like object that reads data from a user-provided
    callback and implements a simple cache to improve performance.

    This class is optimized for sequential read patterns, such as streaming
    video or audio files, where data is often read in contiguous blocks.
    """

    # Note: The caching strategy is a simple FIFO-like mechanism based on insertion order.
    # When a new chunk is read and the buffer is full, the chunk with the smallest
    # starting offset is evicted. This is efficient for sequential reads but may not
    # be ideal for highly random access patterns, where an LRU (Least Recently Used)
    # cache might be more suitable.
    def __init__(self, reader: Callable[[int], tuple[int, bytes]], size: int, buffer_size: float = inf):
        """
        Initializes the CachedCustomFile. The callback function must adhere to the following contract:
            - It accepts one argument: `offset` (int), the position from
              which the data is requested.
            - It returns a tuple of (`read_offset`, `data`):
                - `read_offset` (int): The starting offset of the returned
                  data chunk. This can be different from the requested
                  `offset` (e.g., aligned to a block boundary).
                - `data` (bytes): The chunk of data that was read.
            - **Crucially**, the implementation must guarantee that the
              requested `offset` is contained within the returned chunk, i.e.,
              `read_offset <= offset < read_offset + len(data)`.

        :param reader: A callback function that performs the actual data reading
        :param size: The total size of this virtual file in bytes.
        :param buffer_size: The maximum total size of data chunks to keep in the
            cache. At least one chunk is always kept, even if its size
            exceeds the buffer_size. Defaults to infinity (unlimited buffer).
        """
        self._size = size
        self._pos = 0
        self._reader = reader
        self._remaining_buffer_size = buffer_size
        self._offsets: list[int] = []
        self._data_chunks: list[bytes] = []

    def read(self, size: int = -1) -> bytes:
        """
        Read up to `size` bytes from the stream and return them.

        If `size` is negative or omitted, read all bytes until EOF.

        Returns:
            bytes: The data read from the stream.
        """
        super().read(size)
        if self._pos >= self._size:
            return b""

        bytes_to_read = self._size - self._pos if size == -1 else min(size, self._size - self._pos)
        chunks = []
        while bytes_to_read > 0:
            chunk = self._read_from_single_chunk(bytes_to_read)
            chunk_size = len(chunk)
            chunks.append(chunk)
            bytes_to_read -= chunk_size
            self._pos += chunk_size
        return b''.join(chunks)

    def _read_from_single_chunk(self, size: int, is_retrying: bool = False) -> bytes:
        """
        Reads a piece of data up to `size` bytes, ensuring the data comes from
        a single contiguous (cached or newly fetched) chunk.
        """
        offset = self._pos
        # Find the index of the cached chunk that should contain our offset.
        idx = bisect_right(self._offsets, offset) - 1

        # Check for a cache hit.
        if idx >= 0:
            cache_offset, cache_data = self._offsets[idx], self._data_chunks[idx]
            if cache_offset <= offset < cache_offset + len(cache_data):
                start = offset - cache_offset
                return cache_data[start:start + size]

        # Handle cache miss.
        assert not is_retrying, "Internal error: data is still missing after attempting to fetch it."

        # 1. Fetch a new data chunk from the external reader.
        read_offset, read_data = self._reader(offset)
        self._remaining_buffer_size -= len(read_data)

        # 2. Evict the oldest chunk(s) if the buffer size is exceeded.
        while self._remaining_buffer_size < 0 and self._data_chunks:
            evicted_chunk = self._data_chunks.pop(0)
            self._offsets.pop(0)
            self._remaining_buffer_size += len(evicted_chunk)

        # 3. Insert the new chunk into the cache, maintaining sorted offsets.
        insert_idx = bisect_right(self._offsets, read_offset)
        self._offsets.insert(insert_idx, read_offset)
        self._data_chunks.insert(insert_idx, read_data)

        # 4. Retry the read, which is now guaranteed to be a cache hit.
        return self._read_from_single_chunk(size, is_retrying=True)
