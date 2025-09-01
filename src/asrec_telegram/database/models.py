from collections import namedtuple
from contextlib import asynccontextmanager
from datetime import datetime
from itertools import pairwise
from pathlib import Path
from typing import Union, TypedDict, NotRequired, Optional
from tortoise import Tortoise, fields, connections
from tortoise.models import Model

from ..live_info import resolve_live_raw_name

TORTOISE_ORM = {
    "connections": {"default": "sqlite://db.sqlite3"},
    "apps": {
        "models": {
            "models": ["asrec_telegram.database.models", "aerich.models"],
            "default_connection": "default",
        },
    },
}


class SegInfo(TypedDict):
    file_unique_id: str
    chat_id: int
    message_id: int
    hash: NotRequired[str]
    size: int
    offset: int


class RawFile(Model):
    file_unique_id = fields.CharField(pk=True, max_length=255)
    file = fields.ForeignKeyField('models.File', related_name='segments')
    segment_idx = fields.IntField()
    size = fields.IntField()
    offset = fields.IntField()
    hash = fields.CharField(max_length=255, null=True)
    # telegram info
    chat_id = fields.IntField()
    message_id = fields.IntField()

    class Meta:
        unique_together = (("file", "segment_idx"), ("chat_id", "message_id"))


# class RawFileIDCache(Model):
#     file_unique_id = fields.CharField(pk=True, max_length=255)
#     video = fields.ForeignKeyField('models.File', related_name='segments')
#     segment_idx = fields.IntField()
#     size = fields.IntField()
#     offset = fields.IntField()
#     md5 = fields.CharField(max_length=255)
#     # telegram info
#     chat_id = fields.IntField()
#     message_id = fields.IntField()


class File(Model):
    # db
    id = fields.IntField(pk=True)
    segments = fields.ReverseRelation['RawFile']
    total_segments = fields.IntField()
    live = fields.ForeignKeyField('models.Live', related_name='files')
    # file info
    size = fields.IntField()
    file_folder = fields.CharField(max_length=255)
    file_name = fields.CharField(max_length=255)
    mediainfo = fields.JSONField(default=dict)

    class Meta:
        unique_together = (("live", "file_folder", "file_name"),)

    @staticmethod
    def _validate_segments(size, sorted_segments: list[RawFile]):
        if sorted_segments[0].offset != 0:
            return False
        for si, sj in pairwise(sorted_segments):
            if si.offset + si.size != sj.offset:
                return False
        last_seg = sorted_segments[-1]
        if last_seg.offset + last_seg.size != size:
            return False
        return True

    async def get_segments(self) -> list[RawFile]:
        segments = await self.segments.order_by('segment_idx')
        if len(segments) != self.total_segments or not self._validate_segments(self.size, segments):
            raise RuntimeError(f"{self} is incomplete or corrupted")
        return segments


class Live(Model):
    # file info
    id = fields.IntField(pk=True)
    raw_name = fields.CharField(max_length=255, unique=True)
    files = fields.ReverseRelation['File']
    # live info
    title = fields.CharField(max_length=255)
    artist = fields.CharField(max_length=20)
    start_time = fields.DatetimeField()


async def init():
    await Tortoise.init(
        db_url='sqlite://db.sqlite3',
        modules={'models': ['asrec_telegram.database.models']},
    )
    await Tortoise.generate_schemas()


async def shutdown():
    await connections.close_all()


@asynccontextmanager
async def connect():
    await init()
    try:
        yield
    finally:
        await shutdown()


# async def add_raw_file(file_unique_id: str, chat_id: int, message_id: int, segment_idx: int, size: int):
#     return await RawFile.create(file_unique_id=file_unique_id, chat_id=chat_id, message_id=message_id,
#                                 segment_idx=segment_idx, size=size)


async def add_file(segment: Union[list[SegInfo], SegInfo], live: Live, size: int,
                   file_folder: str, file_name: str, mediainfo: dict):
    if not isinstance(segment, list):
        segment = [segment]
    seg_total_size = sum([si['size'] for si in segment])
    if seg_total_size != size:
        raise ValueError(f"Total size of segments {seg_total_size} does not match provided size {size}.")
    file = await File.create(total_segments=len(segment), live=live, size=size,
                             file_folder=file_folder, file_name=file_name, mediainfo=mediainfo)
    for i, si in enumerate(segment):
        await RawFile.create(**si, file=file, segment_idx=i)


async def get_or_create_live_by_raw_name(raw_name, **kwargs):
    if (live_info := resolve_live_raw_name(raw_name)) is None:
        start_time = 0
        title = artist = ''
    else:
        start_time = datetime.strptime(live_info['date'] + '+0800', '%y%m%d%z')
        title = live_info['title']
        artist = live_info['artist']
    kwargs.setdefault('title', title)
    kwargs.setdefault('artist', artist)
    kwargs.setdefault('start_time', start_time)
    return await Live.get_or_create(kwargs, raw_name=raw_name)


def path_to_named_parts(path: Union[str, Path]):
    live_name, *inner_path = Path(path).parts
    return namedtuple(
        'PathParts', ['live_name', 'file_folder', 'file_name'],
    )(live_name, '/'.join(inner_path[:-1]), inner_path[-1])


async def get_file_by_path(path: Union[str, Path]) -> Optional[File]:
    path_parts = path_to_named_parts(path)
    live = await Live.get_or_none(raw_name=path_parts.live_name)
    if live is not None:
        return await File.get_or_none(
            live=live, file_folder=path_parts.file_folder, file_name=path_parts.file_name
        )
