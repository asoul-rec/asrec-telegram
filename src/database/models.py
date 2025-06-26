from datetime import datetime
from typing import Union, TypedDict, NotRequired
from tortoise import Tortoise, fields, connections
from tortoise.models import Model

from src.live_info import resolve_rec_name

TORTOISE_ORM = {
    "connections": {"default": "sqlite://db.sqlite3"},
    "apps": {
        "models": {
            "models": ["src.database.models", "aerich.models"],
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
    mediainfo = fields.JSONField()

    class Meta:
        unique_together = (("live", "file_folder", "file_name"), )


class Live(Model):
    # file info
    id = fields.IntField(pk=True)
    raw_name = fields.CharField(max_length=255)
    files = fields.ReverseRelation['File']
    # live info
    title = fields.CharField(max_length=255)
    artist = fields.CharField(max_length=20)
    start_time = fields.DatetimeField()


async def init():
    await Tortoise.init(
        db_url='sqlite://db.sqlite3',
        modules={'models': ['src.database.models']},
    )
    await Tortoise.generate_schemas()


async def shutdown():
    await connections.close_all()


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


async def add_live(raw_name, **kwargs):
    if (live := await Live.get_or_none(raw_name=raw_name)) is not None:
        return live
    if (live_info := resolve_rec_name(raw_name)) is None:
        start_time = 0
        title = artist = ''
    else:
        start_time = datetime.strptime(live_info['date'] + '+0800', '%y%m%d%z')
        title = live_info['title']
        artist = live_info['artist']
    kwargs.setdefault('title', title)
    kwargs.setdefault('artist', artist)
    kwargs.setdefault('start_time', start_time)
    return await Live.create(raw_name=raw_name, **kwargs)
