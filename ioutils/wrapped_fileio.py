import io
from typing import Literal
from contextlib import nullcontext, AbstractContextManager


class FilePart(io.IOBase):
    """
    only implement read seek tell, fake close
    """

    def __init__(self, big_file: io.IOBase, offset: int, size: int, close_parent=False, parent_lock=None):
        self._big_file = big_file
        self._base_offset = offset
        self._offset = 0
        self._size = size
        self._close_parent = close_parent
        self._parent_lock: AbstractContextManager = nullcontext() if parent_lock is None else parent_lock

    def seek(self, offset: int, whence: Literal[0, 1, 2] = 0) -> int:
        current_offset = self._offset
        if whence == io.SEEK_SET:
            current_offset = offset
        elif whence == io.SEEK_CUR:
            current_offset += offset
        elif whence == io.SEEK_END:
            current_offset = self._size + offset
        else:
            raise ValueError("Invalid whence")
        if current_offset < 0:
            raise OSError("Invalid seek position")
        self._offset = current_offset
        return self._offset

    def tell(self):
        return self._offset

    def seekable(self):
        return True

    def readable(self):
        return True

    def read(self, size: int = -1) -> bytes:
        if size < -1:
            raise ValueError("read length must be non-negative or -1")
        if self._offset >= self._size:
            return b""
        if size == -1:
            size = self._size - self._offset
        else:
            size = min(size, self._size - self._offset)
        with self._parent_lock:
            self._big_file.seek(self._base_offset + self._offset)
            output = self._big_file.read(size)
            self._offset += size
            return output

    def close(self):
        with self._parent_lock:
            if self._close_parent and not self._big_file.closed:
                self._big_file.close()
        self._big_file = None

    @property
    def closed(self):
        return True if self._big_file is None else self._big_file.closed


# class CombinedFile(io.IOBase):
#     def __init__(self, parts):
#         self._parts = list(parts)
#         self._part_offsets = []
#         self._offset = 0
#         for fp in parts:
#             fp.seek(0, io.SEEK_END)
#
#     def read(self, size: int = -1) -> bytes:
#         if size < -1:
#             raise ValueError("read length must be non-negative or -1")
