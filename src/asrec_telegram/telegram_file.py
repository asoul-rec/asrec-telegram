import logging
from typing import Union
from pathlib import Path

from pyrogram import Client

from .database import get_file_by_path
from .bot.media import get_media_readers
from .ioutils import CachedCustomFile, CombinedFile


async def open_telegram(client: Client, path: Union[str, Path]):
    file = await get_file_by_path(path)
    if file is None:
        raise FileNotFoundError(f"No such file in database: '{path}'")
    # resolve tg api params from path
    chat_id = None
    message_ids = []
    raw_files = await file.get_segments()
    logging.info(f"loading file '{path}' with {len(raw_files)} segments")
    for raw_file in raw_files:
        if chat_id is None:
            chat_id = raw_file.chat_id
        elif chat_id != raw_file.chat_id:
            raise NotImplementedError("only support segmented file in a single chat")
        message_ids.append(raw_file.message_id)
    readers = await get_media_readers(client, chat_id, message_ids)
    # build FileProxy
    segments_fileio = []
    for reader, raw_file in zip(readers, raw_files):
        size, remote_size = raw_file.size, reader.get_size()
        if size != remote_size:
            raise RuntimeError(f"{raw_file} size ({size} bytes) do not match with "
                               f"the media size on Telegram server ({remote_size} bytes)")
        segments_fileio.append(CachedCustomFile(reader.read_threadsafe, size, buffer_size=64 << 20))
    return CombinedFile(segments_fileio)
