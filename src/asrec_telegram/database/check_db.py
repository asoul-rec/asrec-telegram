import re
from collections.abc import Iterable
from pathlib import Path
from typing import Union
import logging

from pyrogram.types import Message

from .models import RawFile, File, get_or_create_live_by_raw_name


async def fix_raw_file(client, chat_id, message_ids: Union[int, Iterable[int]],
                       create_parents=False, continue_on_error=False, **kwargs):
    """
    Check if the raw file exists in the database.
    :param client: bot client
    :param chat_id: id of the chat containing the raw file
    :param message_ids: id of the message(s) containing the raw file
    :param create_parents: if True, create File and Live objects if they do not exist
    :param continue_on_error: if True, only log the error and continue processing other messages
    :param kwargs: additional arguments, should have same length as message_ids
    :return: the file object
    """
    is_iterable = not isinstance(message_ids, int)  # match Pyrogram logic
    messages = await client.get_messages(chat_id, message_ids, replies=0)
    if is_iterable:
        kwargs_list = [dict(zip(kwargs.keys(), vals)) for vals in zip(*kwargs.values())]
        if not kwargs_list:
            kwargs_list = [{} for _ in messages]
        if len(messages) != len(kwargs_list):
            raise ValueError("Length of message_ids and kwargs do not match")
        exec_args = zip(messages, kwargs_list)
    else:
        exec_args = [[messages, kwargs]]
    for message, keys in exec_args:
        try:
            await _fix_raw_file_by_message(message, keys, create_parents)
        except Exception as e:
            logging.error(f"Failed to fix raw file for message {message.id}: {e!r}")
            if not continue_on_error:
                raise

    # else:
    #     await _fix_raw_file_by_message(messages, kwargs, create_parents)


async def _fix_raw_file_by_message(message: Message, keys, create_parents):
    file = message.document or message.video
    if file is None:
        raise ValueError("Message do not contain a document or video")
    if 'segment_idx' not in keys:
        suffix = file.file_name.rsplit('.', 1)[-1]
        keys['segment_idx'] = int(m.group(1)) if (m := re.fullmatch(r'part(\d+)', suffix)) else 0

    if 'file' not in keys:
        location = message.caption
        if location is None:
            raise ValueError("Cannot speculate file info from message without caption")
        live_name, *inner_path = Path(location).parts
        if create_parents:
            live, created = await get_or_create_live_by_raw_name(live_name)
            if created:
                logging.info(f"Created new Live object for {live_name}")
            file_defaults = {
                'total_segments': 1,
                'size': file.file_size
            }
            keys['file'], created = await File.get_or_create(
                file_defaults,
                live=live, file_name=inner_path[-1], file_folder='/'.join(inner_path[:-1])
            )
            if created:
                logging.info(f"Created new File object for {live_name}/"
                             f"{keys['file'].file_folder}/{keys['file'].file_name}")
        else:
            keys['file'] = await File.get_or_none(
                file_name=inner_path[-1], file_folder='/'.join(inner_path[:-1]),
                live__raw_name=live_name
            )
            if keys['file'] is None:
                raise ValueError(f"File {location} does not exist in database")
    if 'offset' not in keys:
        if keys['segment_idx'] == 0:
            keys['offset'] = 0
        else:  # Speculate offset from previous segment
            for other_segment in await keys['file'].segments:
                if other_segment.segment_idx == keys['segment_idx'] - 1:
                    keys['offset'] = other_segment.offset + other_segment.size
                    break
            else:
                raise ValueError(
                    f"Cannot determine offset for segment {keys['segment_idx']} of file "
                    f"{(await keys['file'].live).raw_name}/{keys['file'].file_folder}/{keys['file'].file_name}"
                )

    raw_file_keys = {
        "file_unique_id": file.file_unique_id,
        "size": file.file_size,
    }
    raw_file_keys.update(keys)
    raw_file = await RawFile.get_or_none(chat_id=message.chat.id, message_id=message.id)
    if raw_file is None:
        await RawFile.create(**raw_file_keys, chat_id=message.chat.id, message_id=message.id)
    else:
        await raw_file.update_from_dict(raw_file_keys).save()
