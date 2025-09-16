import asyncio
import logging
from asyncio import run_coroutine_threadsafe, AbstractEventLoop
from collections import defaultdict
from collections.abc import Sequence, AsyncGenerator
from contextlib import nullcontext
from typing import Optional, Union

from pyrogram import Client
from pyrogram.types import Message


def resolve_media(message: Message):
    available_media = ("audio", "document", "photo", "sticker", "animation", "video", "voice", "video_note",
                       "new_chat_photo")
    for kind in available_media:
        media = getattr(message, kind, None)
        if media is not None:
            return media
    raise ValueError("This message doesn't contain any downloadable media")


class MediaReader:
    # Future work: prefetch 1 chunk to reduce jitter?
    _offset = None
    _aiter: Optional[AsyncGenerator[bytes]]
    _client_active_aiter: defaultdict[Client, Optional[AsyncGenerator]] = defaultdict(lambda: None)
    _client_lock: defaultdict[Client, asyncio.Lock] = defaultdict(asyncio.Lock)
    _client_loop: dict[Client, AbstractEventLoop] = {}
    CHUNK_SIZE = 1 << 20  # pyrogram specified chunk size
    TIMEOUT = 5
    MAX_RETRY = 3

    def __init__(self, client: Client, message: Message, loop=None):
        resolve_media(message)  # sanity check
        self.client = client
        self.message = message
        self._aiter = None
        self.loop = asyncio.get_running_loop() if loop is None else loop
        # Ensure consistency for each Client
        if client not in self._client_loop:
            self._client_loop[client] = self.loop
        else:
            if self.loop is not self._client_loop[client]:
                raise RuntimeError("Cannot create MediaReader in different event loops for the same client")

    # Note: We do not intend to download concurrently, so we should ensure there is
    # only 1 active Client.get_file() aiter per Client holding in any MediaReader.
    # Otherwise, the abandoned aiter will block Client.get_file_semaphore.
    async def _replace_aiter(self, ait, acquire_lock=False) -> None:
        ctx = self._client_lock[self.client] if acquire_lock else nullcontext()
        async with ctx:
            old_ait = self._client_active_aiter[self.client]
            if old_ait is not None:
                await old_ait.aclose()
            self._client_active_aiter[self.client] = self._aiter = ait

    def _is_holding_active_aiter(self) -> bool:
        return self._aiter is not None and self._aiter is self._client_active_aiter[self.client]

    async def read_coroutine(self, offset: int) -> tuple[int, bytes]:
        """
        Read from telegram. This is not designed for downloading concurrently with the same Client.
        Although this function is reentrant, severe performance degradation should be expected.
        :param offset: the position from which the data is requested
        :return: tuple (`read_offset`, `data`), the raw bytes (data) and its real offset (read_offset) in the file
        """
        output_chunk_offset = offset // self.CHUNK_SIZE
        output_offset = output_chunk_offset * self.CHUNK_SIZE
        async with self._client_lock[self.client]:
            # Replace aiter when needed
            if self._offset != output_offset or not self._is_holding_active_aiter():
                await self._replace_aiter(self.client.stream_media(self.message, offset=output_chunk_offset))
                self._offset = output_offset
                logging.info(f"Starting a new downloading stream for message {self.message.chat.id}/{self.message.id}, "
                             f"offset {output_offset}")
            else:
                logging.debug(f"Continue downloading new piece with offset {output_offset}")
            # Read from aiter
            data = await anext(self._aiter, None)
            if data is None:
                # Internal logic error: trying to get data from closed stream. Possible causes may include
                # reading beyond the end of current media or Telegram server / upstream error
                raise RuntimeError("media stream is closed")
            self._offset += len(data)
        if len(data) != self.CHUNK_SIZE:
            logging.info(f"Received chunk size {len(data)}. This should be the final piece of the media.")
        return output_offset, data

    def read_threadsafe(self, offset: int) -> tuple[int, bytes]:
        # Caution: always modify self in self.loop by run_coroutine_threadsafe rather than in this thread
        """
        Read in a different thread. Note that this CANNOT be used for running synchronously
        in the same thread of the event loop since that will result in deadlock.
        This implements the reader callback interface for CachedCustomFile
        :param offset: the position from which the data is requested
        :return: tuple (`read_offset`, `data`), the raw bytes (data) and its real offset (read_offset) in the file
        """
        for i in range(self.MAX_RETRY + 1):
            future = run_coroutine_threadsafe(self.read_coroutine(offset), self.loop)
            try:
                return future.result(self.TIMEOUT)
            except Exception as e:
                future.cancel()
                if i < self.MAX_RETRY:
                    logging.warning(f"An exception occurred during reading: {e!r}, "
                                    f"retrying {i + 1}/{self.MAX_RETRY}...")
                    # The current aiter is likely broken so we discard it
                    try:
                        run_coroutine_threadsafe(
                            self._replace_aiter(None, acquire_lock=True), self.loop).result(self.TIMEOUT)
                    except TimeoutError:
                        logging.error(f"Timeout when discarding the active aiter "
                                      f"after trying to cancel it. Exiting...")
                        break
                else:
                    logging.error(f"An exception occurred during reading: {e!r}. Exiting...")
        raise IOError("Failed to load data from Telegram")

    def get_size(self) -> Optional[int]:
        try:
            return resolve_media(self.message).file_size
        except ValueError:
            pass


async def get_media_readers(
        client: Client, chat_id, message_ids: Union[int, Sequence[int]]
) -> Union[MediaReader, list[MediaReader]]:
    """
    Build media readers from Telegram message ids
    :param client: Pyrogram Client
    :param chat_id: A unique identifier or username of the target chat
    :param message_ids: A single message identifier or an iterable of message ids (as integers).
    :return: a single media reader or a list of media readers, following the behavior of Client.get_messages().
    """
    messages = await client.get_messages(chat_id, message_ids)
    if not isinstance(messages, list):
        return MediaReader(client, messages)
    else:
        return [MediaReader(client, m) for m in messages]
