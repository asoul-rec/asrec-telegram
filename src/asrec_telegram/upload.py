# import os.path
import io
from pathlib import Path
import logging
from typing import Union

from pyrogram import Client
from pyrogram.enums import ParseMode
from .database import SegInfo, add_file, get_or_create_live_by_raw_name, path_to_named_parts
from .ioutils.wrapped_fileio import PartedFile


async def upload_file(client: Client, chat_id: int,
                      base_dir: Union[str, Path], location: Union[str, Path], size_limit=2000 << 20):
    def progress(c, t):
        logging.debug(f"uploading '{location}', {c / 1048576:.2f} / {t / 1048576:.2f} MB")

    async def upload(fp):
        kwargs = {'chat_id': chat_id, 'caption': location, 'parse_mode': ParseMode.DISABLED, 'progress': progress}
        try:
            if is_video:
                message_upload = await client.send_video(video=fp, **kwargs)
            else:
                message_upload = await client.send_document(document=fp, force_document=True, **kwargs)
        except Exception as e:
            logging.error(f"failed to upload '{location}': {e!r}")
            return

        if message_upload is None:
            logging.warning(f"cancelled uploading '{location}'")
            return
        logging.info(f"successfully uploaded '{location}'")
        return message_upload

    if not isinstance(chat_id, int):
        chat_id = (await client.get_chat(chat_id)).id

    file_path = Path(base_dir) / location
    file_stat = file_path.stat()
    location_parts = path_to_named_parts(location)

    file_size = file_stat.st_size
    is_video = file_size <= size_limit and file_path.suffix[1:] in ['mp4', 'webm', 'flv', 'mkv', 'ts']

    with open(file_path, 'rb') as f:
        f: io.BufferedReader
        segments = []
        for i, f_part in enumerate(PartedFile.split_file(f, size_limit, file_size=file_size)):
            if is_video:
                f_part.name = file_path.stem
            else:
                f_part.name = file_path.name
                if i > 0:
                    f_part.name += f".part{i}"
            message = await upload(f_part)
            uploaded_file = message.video if is_video else message.document
            assert uploaded_file.file_size == f_part.size
            segments.append(SegInfo(
                file_unique_id=uploaded_file.file_unique_id, chat_id=chat_id, message_id=message.id,
                size=f_part.size, offset=f_part.offset
            ))
    await add_file(
        segment=segments,
        live=await get_or_create_live_by_raw_name(raw_name=location_parts.live_name),
        size=file_size,
        file_folder=location_parts.file_folder,
        file_name=location_parts.live_name, mediainfo={}
    )
