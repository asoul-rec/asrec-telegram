import asyncio
import logging
from asyncio import run_coroutine_threadsafe, AbstractEventLoop
from collections.abc import AsyncIterable, Sequence, AsyncGenerator
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
    _offset = None
    _aiter: Optional[AsyncGenerator[bytes]]
    _global_active_aiter: Optional[AsyncGenerator] = None
    CHUNK_SIZE = 1 << 20  # pyrogram specified chunk size
    TIMEOUT = 10

    def __init__(self, client: Client, message: Message, loop=None):
        self.client = client
        self.message = message
        self.loop = asyncio.get_running_loop() if loop is None else loop
        self._aiter = None

    # Note: We do not intend to download concurrently, so we should ensure there is
    # only 1 active Client.get_file() aiter globally for MediaReader.
    # Otherwise, the abandoned aiter will still block Client.get_file_semaphore.
    @classmethod
    async def _replace_active_aiter(cls, ait):
        if cls._global_active_aiter is not None:
            await cls._global_active_aiter.aclose()
            # if loop != old_loop:
            #     raise RuntimeError("All MediaReader reads must run in the same loop globally.")
        cls._global_active_aiter = ait

    # Note: We must create stream_media coroutine in the event loop running in main thread.
    # Otherwise, Pyrogram will apply async_to_sync magic on it.
    async def _read_coroutine(self, replace_chunk_offset=None):
        if replace_chunk_offset is not None:
            self._aiter = self.client.stream_media(self.message, offset=replace_chunk_offset)
            await self._replace_active_aiter(self._aiter)
        data = await anext(self._aiter, None)
        if data is None:
            raise EOFError("reached the end of current media")
        return data

    def read_threadsafe(self, offset: int) -> tuple[int, bytes]:
        output_chunk_offset = offset // self.CHUNK_SIZE
        output_offset = output_chunk_offset * self.CHUNK_SIZE
        if self._offset != output_offset or self._aiter is None:
            self._offset = output_offset
            new_chunk_offset = output_chunk_offset
            logging.info(f"Starting a new downloading stream for message {self.message.chat.id}/{self.message.id}, "
                         f"offset {output_offset}")
        else:
            new_chunk_offset = None
            logging.debug(f"Continue downloading new piece with offset {output_offset}")

        future = run_coroutine_threadsafe(self._read_coroutine(new_chunk_offset), self.loop)
        result = future.result(self.TIMEOUT)
        self._offset += len(result)
        if len(result) != self.CHUNK_SIZE:
            logging.info(f"Received chunk size {len(result)}. This should be the final piece of the media.")
        return output_offset, result

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
