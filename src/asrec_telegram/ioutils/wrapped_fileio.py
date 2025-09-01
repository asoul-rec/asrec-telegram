import io
from abc import ABC, abstractmethod
from bisect import bisect_right
from collections.abc import Sequence
from contextlib import nullcontext, AbstractContextManager
from itertools import accumulate
from typing import Literal


class FileProxy(io.IOBase, ABC):
    """
    An abstract base class for file-like proxy objects, providing
    common seeking and telling functionality.
    """
    _size: int  # Total size of this virtual file in bytes
    _pos: int  # Internal cursor position for this part, from 0 to self._size

    @property
    def size(self) -> int:
        """The total size of this virtual file."""
        return self._size

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def tell(self) -> int:
        """Return the current stream position."""
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return self._pos

    def seek(self, offset: int, whence: Literal[0, 1, 2] = 0) -> int:
        """
        Change the stream position.

        :param offset: The byte offset.
        :param whence: The reference point (io.SEEK_SET, io.SEEK_CUR, io.SEEK_END).
        :return: The new absolute position in this virtual file.
        """
        if self.closed:
            raise ValueError("I/O operation on closed file.")

        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == io.SEEK_END:
            new_pos = self._size + offset
        else:
            raise ValueError(f"Invalid whence value: {whence}")

        if new_pos < 0:
            raise OSError(f"Negative seek position {new_pos} is invalid")

        self._pos = new_pos
        return self._pos

    # noinspection PyTypeChecker
    @abstractmethod
    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if size < -1:
            raise ValueError("read length must be non-negative or -1")


class PartedFile(FileProxy):
    """
    A file-like object that represents a part of a larger,
    underlying readable and seekable file.
    """

    def __init__(self, big_file: io.IOBase, offset: int, size: int, close_parent: bool = False, parent_lock=None):
        """
        Initializes the PartedFile.

        :param big_file: The large, underlying file-like object. Must be seekable and readable.
        :param offset: The starting byte offset of this part within the big_file.
        :param size: The total size in bytes of this part.
        :param close_parent: If True, closing this object will also close the big_file.
        :param parent_lock: A lock to ensure thread-safe access to the big_file.
        """
        if not big_file.seekable() or not big_file.readable():
            raise ValueError("The underlying file must be readable and seekable.")

        self._big_file = big_file
        self._base_offset = offset
        self._size = size
        self._close_parent = close_parent
        self._parent_lock: AbstractContextManager = nullcontext() if parent_lock is None else parent_lock
        self._pos = 0

    @property
    def offset(self) -> int:
        """
        The byte offset of this part within the big_file.
        """
        return self._base_offset

    def read(self, size: int = -1) -> bytes:
        """
        Read up to `size` bytes from the file part and return them.
        :param size: The number of bytes to read. If -1, reads to the end of the part.
        :return: The bytes read from the file part.
        """
        super().read(size)
        remaining_in_part = self._size - self._pos
        if remaining_in_part <= 0:
            return b""

        read_size = remaining_in_part if size == -1 else min(size, remaining_in_part)

        with self._parent_lock:
            self._big_file.seek(self._base_offset + self._pos)
            output = self._big_file.read(read_size)

        # Update internal offset based on the actual number of bytes read
        self._pos += len(output)
        return output

    def close(self):
        """
        Close the file part. If close_parent was set to True, this also
        closes the underlying big_file.
        """
        if self._close_parent and self._big_file:
            with self._parent_lock:
                if not self._big_file.closed:
                    self._big_file.close()
        self._big_file = None

    @property
    def closed(self) -> bool:
        return self._big_file is None or self._big_file.closed

    @classmethod
    def split_file(cls, big_file: io.IOBase, part_size: int, *,
                   file_size=None, close_parent: bool = False) -> list['PartedFile']:
        """
        Split a large file into smaller parts of specified size.

        :param big_file: The large file to split.
        :param part_size: The size of each part in bytes.
        :param file_size: Optional total size of the big_file.
          If not provided, it will be determined by seeking to the end.
        :param close_parent: If True, closing the returned parts will also close the big_file.
        :return: A list of PartedFile objects representing the parts of the big_file.
        """
        if not big_file.seekable() or not big_file.readable():
            raise ValueError("The underlying file must be readable and seekable.")

        parts = []
        if file_size is None:
            big_file.seek(0, io.SEEK_END)
            total_size = big_file.tell()
            big_file.seek(0)
        else:
            total_size = file_size

        for offset in range(0, total_size, part_size):
            size = min(part_size, total_size - offset)
            parts.append(cls(big_file, offset, size, close_parent=close_parent))

        return parts


class CombinedFile(FileProxy):
    """
    A file-like object that combines multiple file-like objects
    into a single, continuous virtual file.
    """

    def __init__(self, parts: Sequence[io.IOBase], close_parts: bool = False):
        """
        Initializes the CombinedFile.
        :param parts: A sequence of readable and seekable file-like objects.
        :param close_parts: If True, closing this CombinedFile will also close all parts.
        """
        self._parts = list(parts)
        self._close_parts = close_parts
        self._part_sizes = []
        for part in self._parts:
            if not part.readable() or not part.seekable():
                raise ValueError("All parts must be readable and seekable.")
            part.seek(0, io.SEEK_END)
            self._part_sizes.append(part.tell())
        self._part_cumulative_offsets = list(accumulate(self._part_sizes, initial=0))
        self._size = self._part_cumulative_offsets[-1]
        self._pos = 0

    def _find_part(self, offset: int):
        """Find the part index and the local offset corresponding to a global offset."""
        # Ensures self._pco[part_idx] <= offset and self._pco[part_idx + 1] > offset when valid
        # This means self._parts[part_idx] contains the byte data at this offset
        part_idx = bisect_right(self._part_cumulative_offsets, offset) - 1
        if not (0 <= part_idx < len(self._parts)):
            return None, None
        local_offset = offset - self._part_cumulative_offsets[part_idx]
        return part_idx, local_offset

    def read(self, size: int = -1) -> bytes:
        super().read(size)
        if self._pos >= self._size:
            return b""

        bytes_to_read = self._size - self._pos if size == -1 else min(size, self._size - self._pos)

        chunks = []
        while bytes_to_read > 0:
            part_idx, local_offset = self._find_part(self._pos)
            assert part_idx is not None, f"invalid position {self._pos} for CombinedFile.read()"
            part = self._parts[part_idx]
            part.seek(local_offset)

            part_size = self._part_sizes[part_idx]
            read_from_this_part = min(bytes_to_read, part_size - local_offset)
            assert read_from_this_part > 0  # always read something, otherwise we will loop forever

            chunk = part.read(read_from_this_part)
            if len(chunk) != read_from_this_part:
                raise RuntimeError(f"Expected to read {read_from_this_part} but got {len(chunk)} bytes. "
                                   f"The underlying file might be mutated")
            chunks.append(chunk)
            bytes_to_read -= read_from_this_part
            self._pos += read_from_this_part

        return b"".join(chunks)

    def close(self):
        """
        Close this virtual file. If close_parts was set to True, this also
        closes all the underlying part files.
        """
        if self._close_parts:
            for part in self._parts:
                part.close()
        self._parts = []  # Mark as closed by clearing the list of parts

    @property
    def closed(self) -> bool:
        return not self._parts
